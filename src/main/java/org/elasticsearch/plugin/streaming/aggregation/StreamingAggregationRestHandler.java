package org.elasticsearch.plugin.streaming.aggregation;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
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
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.internal.*;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

import static org.elasticsearch.rest.RestRequest.Method.*;


public class StreamingAggregationRestHandler extends BaseRestHandler implements RestHandler {
    private MemoryResultStorage memoryResultStorage;
    private IndicesService indicesService;
    private BigArrays bigArrays;
    List<IndexReader> memoryIndices = new ArrayList<IndexReader>();
    private Object locker = new Object();

    @Inject
    public StreamingAggregationRestHandler(RestController restController, MemoryResultStorage memoryResultStorage, Settings settings, RestController controller, Client client, IndicesService indicesService, BigArrays bigArrays) {
        super(settings, controller, client);
        this.bigArrays = bigArrays;
        this.memoryResultStorage = memoryResultStorage;
        this.indicesService = indicesService;

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
                createContexHandle(request, channel);
            } else {
                String id = request.param("id");
                SearchRequest searchRequest = RestSearchAction.parseSearchRequest(memoryResultStorage.getRequest(id));
                logger.info(""+request.method(), " --- ");
                if (request.method() == POST || request.method() == PUT) {
                    newDocumentRequestHandle(request, channel, id, searchRequest, memoryResultStorage.getRequest(id));
                } else if(request.method() == DELETE){
                    deleteRequestHandle(channel, client, id, searchRequest);
                }else{
                    resultRequestHandle(channel, client, id, searchRequest);
                }
            }
    }
    private void deleteRequestHandle(RestChannel channel, Client client, String id, SearchRequest searchRequest){
        memoryResultStorage.remove(id);
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, "DELETED"));
    }
    private void createContexHandle(RestRequest request, RestChannel channel) {
        synchronized (locker) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
            //String id = "search" + System.currentTimeMillis();
            String id = "search" + 1;// use this to test
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
            client.search(searchRequest, aggregationRestStatusToXContentListener);
        }
    }

    private void newDocumentRequestHandle(RestRequest request, RestChannel channel, String id, SearchRequest searchRequest, RestRequest restRequest) {
        try {
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
                SearchParse.main(searchRequest, settings, slowSearcher, bigArrays, restRequest, topDocs,  topDocs.scoreDocs);
                //search
                logger.info(Integer.toString(topDocs.totalHits), "");
            } catch (Exception e) {
                logger.error("Erro",e );
            }
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "OK"));
        }catch (Exception e){
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "IGNORED"));
            logger.error("Erro",e );
        }
    }
}
