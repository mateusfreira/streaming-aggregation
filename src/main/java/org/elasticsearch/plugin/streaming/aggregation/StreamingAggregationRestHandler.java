package org.elasticsearch.plugin.streaming.aggregation;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugin.streaming.aggregation.rest.AggregationRestStatusToXContentListener;
import org.elasticsearch.plugin.streaming.aggregation.result.MemoryResultStorage;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.PUT;


public class StreamingAggregationRestHandler extends BaseRestHandler implements RestHandler {
    private MemoryResultStorage memoryResultStorage;
    private IndicesService indicesService;
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

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
            if (request.hasParam("createContext")) {
                createContexHandle(request, channel);
            } else {
                String id = request.param("search_id");
                SearchRequest searchRequest = RestSearchAction.parseSearchRequest(memoryResultStorage.getRequest(id));
                if (request.hasParam("newDocument")) {
                    newDocumentRequestHandle(request, channel, id, searchRequest);
                } else {
                    resultRequestHandle(channel, client, id, searchRequest);
                }
            }
    }

    private void createContexHandle(RestRequest request, RestChannel channel) {
        synchronized (locker) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
            String id = "search" + System.currentTimeMillis();
            //String id = "search" + 1; use this to test
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

    private void newDocumentRequestHandle(RestRequest request, RestChannel channel, String id, SearchRequest searchRequest) {
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
    }
}
