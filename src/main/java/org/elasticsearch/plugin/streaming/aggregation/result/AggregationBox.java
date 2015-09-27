package org.elasticsearch.plugin.streaming.aggregation.result;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.streaming.aggregation.result.exception.NoResponseException;
import org.elasticsearch.rest.RestRequest;

import javax.naming.directory.SearchResult;

/**
 * Created by mateus on 27/09/15.
 */
public class AggregationBox {

    private RestRequest restRequest;
    private SearchResponse response;

    public RestRequest getRestRequest() {
        return restRequest;
    }

    public void setRestRequest(RestRequest restRequest) {
        this.restRequest = restRequest;
    }

    public SearchResponse getResponse() {
        if(!hasResponse()){
            throw new NoResponseException();
        }
        return response;
    }

    public void setResponse(SearchResponse response) {
        this.response = response;
    }
    public boolean hasResponse(){
        return response != null;
    }
    public void clearResponse(){
        setResponse(null);
    }
}
