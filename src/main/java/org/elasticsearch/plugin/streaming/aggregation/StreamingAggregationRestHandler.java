package org.elasticsearch.plugin.streaming.aggregation;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Counter;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.streaming.aggregation.rest.AggregationRestStatusToXContentListener;
import org.elasticsearch.plugin.streaming.aggregation.result.MemoryResultStorage;
import org.elasticsearch.plugin.streaming.aggregation.test.SearchParse;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.internal.*;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

import static org.elasticsearch.rest.RestRequest.Method.*;


public class StreamingAggregationRestHandler extends BaseRestHandler implements RestHandler {
    public static final String DATA = "data";
    private final Injector injector;
    private final SearchPhaseController searchPhaseController;
    private final ClusterService clusterService;
    private MemoryResultStorage memoryResultStorage;
    private IndicesService indicesService;
    private BigArrays bigArrays;
    List<IndexReader> memoryIndices = new ArrayList<IndexReader>();
    private Object locker = new Object();

    @Inject
    public StreamingAggregationRestHandler(RestController restController, MemoryResultStorage memoryResultStorage, Settings settings, RestController controller, Client client, IndicesService indicesService, BigArrays bigArrays, Injector injector, SearchPhaseController searchPhaseController, ClusterService clusterService) {
        super(settings, controller, client);
        this.bigArrays = bigArrays;
        this.clusterService = clusterService;
        this.searchPhaseController = searchPhaseController;
        this.memoryResultStorage = memoryResultStorage;
        this.indicesService = indicesService;
        this.injector = injector;

        //POST		/<index>/.aggregator
        restController.registerHandler(POST, "/{index}/.aggregator", this);

        //GET		/<index>/.aggregator/<id>
        restController.registerHandler(GET, "/{index}/.aggregator", this);
        restController.registerHandler(GET, "/{index}/.aggregator/{id}/_result", this);

        //PUT/POST	/<index>/.aggregator/<id>
        restController.registerHandler(PUT, "/{index}/.aggregator/{id}", this);
        restController.registerHandler(POST, "/{index}/.aggregator/{id}", this);

        //DELETE	/<index>/.aggregator/<id>
        restController.registerHandler(DELETE, "/{index}/.aggregator/{id}", this);
        //registerLifecycleHandler();
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
            if (request.method() == POST && !request.hasParam("id")) {
                createContexHandle(request, channel, client);
            } else {
                String id = request.param("id");
                SearchRequest searchRequest = RestSearchAction.parseSearchRequest(memoryResultStorage.getRequest(id));
                logger.info(""+request.method(), " --- ");
                if (request.method() == POST || request.method() == PUT) {
                    newDocumentRequestHandle(request, channel, id, searchRequest, memoryResultStorage.getRequest(id), client);
                } else if(request.method() == DELETE){
                    deleteRequestHandle(channel, client, id, searchRequest);
                }else{
                    resultRequestHandle(channel, client, id, searchRequest);
                }
            }
    }
    private void deleteRequestHandle(RestChannel channel, Client client, String id, SearchRequest searchRequest){
        memoryResultStorage.remove(id);
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(id);
        deleteIndexRequest.listenerThreaded(false);
        client.admin().indices().delete(deleteIndexRequest);
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, "DELETED"));
    }
    private void createContexHandle(RestRequest request, RestChannel channel, Client client) {
        synchronized (locker) {
            final Settings settings = ImmutableSettings.settingsBuilder()
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                    .build();
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
            String id = "search" + System.currentTimeMillis();
            //String id = "search" + 1;// use this to test
            //indicesService.createIndex(id, settings, clusterService.localNode().id());
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(id);
            createIndexRequest.listenerThreaded(false);
            if (request.hasContent()) {
                createIndexRequest.source(request.content());
            }
            createIndexRequest.timeout(request.paramAsTime("timeout", createIndexRequest.timeout()));
            createIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", createIndexRequest.masterNodeTimeout()));
            client.admin().indices().create(createIndexRequest);
            memoryResultStorage.putRequest(id, request);

            channel.sendResponse(new BytesRestResponse(RestStatus.OK, id));
        }
    }

    private void resultRequestHandle(RestChannel channel, Client client, String id, SearchRequest searchRequest) {

        AggregationRestStatusToXContentListener aggregationRestStatusToXContentListener = new AggregationRestStatusToXContentListener(channel, memoryResultStorage, id);
        logger.info(id, id);
        if (memoryResultStorage.hasResponse(id)) {
            logger.info("From cache", id);
            aggregationRestStatusToXContentListener.onResponse(memoryResultStorage.getResponse(id));
        } else {
            logger.info("From client", id);
            searchRequest.indices(id);
            client.search(searchRequest, aggregationRestStatusToXContentListener);
        }
    }

    private void newDocumentRequestHandle(RestRequest request, RestChannel channel, String id, SearchRequest searchRequest, RestRequest restRequest, Client client) {
        try {
            AggregationRestStatusToXContentListener aggregationRestStatusToXContentListener = new AggregationRestStatusToXContentListener(channel, memoryResultStorage, id);
            IndexRequest indexRequest = new IndexRequest(id, DATA, null);
            indexRequest.source(request.content(), request.contentUnsafe());
            client.index(indexRequest);
            /*
            AggregationRestStatusToXContentListener aggregationRestStatusToXContentListener = new AggregationRestStatusToXContentListener(channel, memoryResultStorage, id);
            //Need lots of improvements....
            memoryResultStorage.clearResponse(id);
            Map<String, Object> upsertDoc = XContentHelper.convertToMap(request.content(), true).v2();
            Iterator<String> keys = upsertDoc.keySet().iterator();
            MemoryIndex memoryIndex = new MemoryIndex();
            while (keys.hasNext()) {
                String key = keys.next();
                String value = upsertDoc.get(key).toString();
                memoryIndex.addField(key, value, new WhitespaceAnalyzer());
            }
            memoryIndices.add(memoryIndex.createSearcher().getIndexReader());
            MultiReader mReader = new MultiReader( memoryIndices.toArray(new IndexReader[memoryIndices.size()]), true);
            final IndexSearcher slowSearcher = new IndexSearcher(SlowCompositeReaderWrapper.wrap(mReader));

            IndexQueryParserService queryParserService = indicesService.indexService(searchRequest.indices()[0]).queryParserService();
            ParsedQuery parsedQuery = queryParserService.parse("{   \"filtered\" : { } }");
            try {
                TopDocs topDocs = slowSearcher.search(parsedQuery.query(), 10);
                //SearchRequest searchRequest, Settings settings, IndexSearcher searcher, BigArrays bigArrays,  RestRequest request, TopDocs topDocs, ScoreDoc[] scoreDocs
                SearchResponse searchResponse =  SearchParse.main(searchRequest, slowSearcher, bigArrays, restRequest, topDocs, topDocs.scoreDocs, injector, client, logger);
                //searchPhaseController.merge(topDocs.scoreDocs, query);
                logger.info(Integer.toString(topDocs.totalHits), "");

                aggregationRestStatusToXContentListener.onResponse(searchResponse);
            } catch (Exception e) {
                logger.error("Erro",e );
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, "OK"));
            }
            //channel.sendResponse(new BytesRestResponse(RestStatus.OK, "OK"));
            */
        }catch (Exception e){
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "IGNORED"));
            logger.error("Erro",e );
        }
    }
}
