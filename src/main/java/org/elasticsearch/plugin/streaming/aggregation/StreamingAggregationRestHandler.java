    package org.elasticsearch.plugin.streaming.aggregation;
    import org.elasticsearch.action.search.SearchRequest;
    import org.elasticsearch.action.search.SearchResponse;
    import org.elasticsearch.client.Client;

    import org.elasticsearch.common.settings.Settings;
    import org.elasticsearch.index.engine.Engine;
    import org.elasticsearch.index.indexing.IndexingOperationListener;
    import org.elasticsearch.index.shard.IndexShard;
    import org.elasticsearch.indices.IndicesLifecycle;
    import org.elasticsearch.indices.IndicesService;
    import org.elasticsearch.plugin.streaming.aggregation.result.MemoryResultStorage;
    import org.elasticsearch.rest.*;

    import org.elasticsearch.common.inject.Inject;
    import org.elasticsearch.rest.action.search.RestSearchAction;
    import org.elasticsearch.rest.action.support.RestStatusToXContentListener;



    import static org.elasticsearch.rest.RestRequest.Method.GET;
    import static org.elasticsearch.rest.RestRequest.Method.PUT;


    public class StreamingAggregationRestHandler extends BaseRestHandler implements RestHandler {
        private MemoryResultStorage memoryResultStorage;
        private IndicesService indicesService;
        SearchRequest searchRequest;
        private Client client;
        private RestChannel channel;

        @Inject
        public StreamingAggregationRestHandler(RestController restController, MemoryResultStorage memoryResultStorage, Settings settings, RestController controller, Client client, IndicesService indicesService) {
            super(settings, controller, client);
            restController.registerHandler(GET, "/{index}/_hello", this);
            restController.registerHandler(PUT, "/{index}/_hello", this);
            this.memoryResultStorage = memoryResultStorage;
            this.indicesService = indicesService ;
            registerLifecycleHandler();
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

            }});
        }

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
            this.client = client;
            this.channel = channel;
            searchRequest  = RestSearchAction.parseSearchRequest(request);
            /*
            String who = request.param("who");
            String whoSafe = (who!=null) ? who : "world";
            memoryResultStorage.addResult(whoSafe);
            SearchResult seraSearchResult = new SearchResult();
            String result = "";
            Iterator<String> results = memoryResultStorage.results().iterator();
            while(results.hasNext()){
                result += results.next() + "\n";
            }
            channel.sendResponse(new BytesRestResponse(OK, result));
            */
        }
    }
