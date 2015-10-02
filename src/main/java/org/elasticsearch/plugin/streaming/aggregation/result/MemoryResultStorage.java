package org.elasticsearch.plugin.streaming.aggregation.result;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.streaming.aggregation.result.exception.NoResponseException;
import org.elasticsearch.rest.RestRequest;

import javax.naming.directory.SearchResult;
import java.util.HashMap;

import static org.elasticsearch.common.logging.Loggers.getLogger;

/**
 * Created by mateus on 21/09/15.
 */

public class MemoryResultStorage {
    private  Settings settings;
    private  ESLogger logger;

    @Inject
    public MemoryResultStorage(Settings settings){
        this.logger = getLogger(this.getClass(), settings, new String[0]);
        this.settings = settings;
        this.searchs = new HashMap();
    }
    private volatile HashMap<String, AggregationBox> searchs;


    public void put(String id, AggregationBox request) {
        this.searchs.put(id, request);
    }

    public void putRequest(String id, RestRequest request) {
        AggregationBox aggregationBox = null;
        if (!this.searchs.containsKey(id)) {
            aggregationBox = new AggregationBox();
            this.put(id, aggregationBox);
        }else {
            aggregationBox = this.get(id);
        }
        aggregationBox.setRestRequest(request);
    }

    public boolean hasSearch(String id) {
        return this.searchs.containsKey(id);
    }

    public void clearResponse(String id) {
        this.get(id).clearResponse();
    }

    public boolean hasResponse(String id) {
        logger.info("hasResponse" + this.get(id).hasResponse());
        return this.hasSearch(id) && this.get(id).hasResponse();
    }

    public SearchResponse getResponse(String id) {
        if (!this.hasSearch(id)) {
            throw new NoResponseException();
        }
        return this.get(id).getResponse();
    }

    public void putResponse(String id, SearchResponse response) {
        this.get(id).setResponse(response);
    }

    public AggregationBox get(String id) {
        return this.searchs.get(id);
    }

    public RestRequest getRequest(String id) {
        return this.searchs.get(id).getRestRequest();
    }

    public void clearResults() {
        searchs.clear();
    }

    public void remove(String id) {
        searchs.remove(id);
    }
}
