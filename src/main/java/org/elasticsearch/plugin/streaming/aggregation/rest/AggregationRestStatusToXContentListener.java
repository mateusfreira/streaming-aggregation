package org.elasticsearch.plugin.streaming.aggregation.rest;

/**
 * Created by mateus on 27/09/15.
 */

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.streaming.aggregation.result.MemoryResultStorage;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestResponseListener;


public class AggregationRestStatusToXContentListener extends RestResponseListener<SearchResponse> {

    private MemoryResultStorage memoryResultStorage;
    private String searchId;
    public AggregationRestStatusToXContentListener(RestChannel channel, MemoryResultStorage memoryResultStorage, String searchId) {
        super(channel);
        this.memoryResultStorage = memoryResultStorage;
        this.searchId = searchId;
    }

    public final RestResponse buildResponse(SearchResponse response) throws Exception {
        return this.buildResponse(response, this.channel.newBuilder());
    }

    public final RestResponse buildResponse(SearchResponse response, XContentBuilder builder) throws Exception {
        builder.startObject();
        response.toXContent(builder, this.channel.request());
        builder.endObject();
        memoryResultStorage.putResponse(searchId, response);
        return new BytesRestResponse(response.status(), builder);
    }
}
