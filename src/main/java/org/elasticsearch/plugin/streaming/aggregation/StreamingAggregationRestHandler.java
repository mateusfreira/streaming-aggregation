package org.elasticsearch.plugin.streaming.aggregation;
import org.elasticsearch.rest.*;

import org.elasticsearch.common.inject.Inject;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class StreamingAggregationRestHandler implements RestHandler {
    @Inject
    public StreamingAggregationRestHandler(RestController restController) {
        restController.registerHandler(GET, "/_hello", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        String who = request.param("who");
        String whoSafe = (who!=null) ? who : "world";
        channel.sendResponse(new BytesRestResponse(OK, "Hello, " + whoSafe + "!".getBytes()));
    }
}