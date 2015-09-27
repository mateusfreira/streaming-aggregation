package org.elasticsearch.plugin.streaming.aggregation;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugin.streaming.aggregation.rest.AggregationRestStatusToXContentListener;
import org.elasticsearch.plugin.streaming.aggregation.result.MemoryResultStorage;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;

import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.PUT;


public class StreamingAggregationRestHandler extends BaseRestHandler implements RestHandler {
    SearchRequest searchRequest;
    private MemoryResultStorage memoryResultStorage;
    private IndicesService indicesService;
    private Client client;
    private RestChannel channel;
    private Object locker = new Object();

    @Inject
    public StreamingAggregationRestHandler(RestController restController, MemoryResultStorage memoryResultStorage, Settings settings, RestController controller, Client client, IndicesService indicesService) {
        super(settings, controller, client);
        restController.registerHandler(GET, "/{index}/_streaming_aggregation", this);
        restController.registerHandler(PUT, "/{index}/_streaming_aggregation", this);
        restController.registerHandler(PUT, "/{type}/{index}/{id}/_streaming_aggregation", this);
        this.memoryResultStorage = memoryResultStorage;
        this.indicesService = indicesService;
        //registerLifecycleHandler();
    }

    private void registerLifecycleHandler() {

        indicesService.indicesLifecycle().addListener(new IndicesLifecycle.Listener() {
            @Override
            public void afterIndexShardStarted(IndexShard indexShard) {
                if (indexShard.routingEntry().primary()) {
                    logger.info("Here", "I'm");

                    indexShard.indexingService().addListener(new IndexingOperationListener() {
                        @Override
                        public void postCreate(Engine.Create create) {
                            logger.info("Here -> Create ", "I'm");
                            client.search(searchRequest, new RestStatusToXContentListener<SearchResponse>(channel));
                            super.postCreate(create);
                        }

                        @Override
                        public void postIndex(Engine.Index index) {
                            logger.info("Here -> postIndex ", "I'm");
                            client.search(searchRequest, new RestStatusToXContentListener<SearchResponse>(channel));
                        }

                        @Override
                        public void postDelete(Engine.Delete delete) {
                            logger.info("Here -> postDelete ", "I'm");
                            client.search(searchRequest, new RestStatusToXContentListener<SearchResponse>(channel));
                            super.postDelete(delete);
                        }
                    });

                }

            }
        });
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
        this.client = client;
        this.channel = channel;
        try {
            if (request.hasParam("createContext")) {
                synchronized (locker) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                    }
                    //String id = "search" + System.currentTimeMillis(); remove this comentary and the folow line
                    String id = "search" + 1;
                    memoryResultStorage.putRequest(id, request);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, id));
                }
            } else {

                String id = request.param("search_id");
                logger.info(id, id);
                searchRequest = RestSearchAction.parseSearchRequest(memoryResultStorage.getRequest(id));
                logger.info(new String(searchRequest.source().toBytes()), "");
                if (request.hasParam("newDocument")) {
                    memoryResultStorage.clearResponse(id);
                    Map<String, Object> upsertDoc = XContentHelper.convertToMap(request.content(), true).v2();
                    Iterator<String> keys = upsertDoc.keySet().iterator();
                    MemoryIndex memoryIndex = new MemoryIndex();
                    while (keys.hasNext()) {
                        String key = keys.next();
                        String value = upsertDoc.get(key).toString();
                        memoryIndex.addField(key, value, new WhitespaceAnalyzer());
                    }
                    IndexQueryParserService queryParserService = indicesService.indexService(searchRequest.indices()[0]).queryParserService();
                    ParsedQuery parsedQuery = queryParserService.parse(searchRequest.source());
                    //logger.info(memoryIndex.toString(), "");
                    try {
                        TopDocs topDocs = memoryIndex.createSearcher().search(parsedQuery.query(), 1);
                        //search
                        logger.info(Integer.toString(topDocs.totalHits), "");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "OK"));
                } else {
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
            }
        } catch (Exception e) {
            logger.error("erro", e);
        }
    }
}
