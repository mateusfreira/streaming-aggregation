package org.elasticsearch.plugin.streaming.aggregation.test;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Counter;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.CacheRecyclerModule;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.discovery.local.LocalDiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.*;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.aliases.IndexAliasesServiceModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.cache.fixedbitset.FixedBitSetFilterCache;
import org.elasticsearch.index.codec.CodecModule;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatService;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexEngine;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataModule;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.IndexGatewayModule;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.functionscore.FunctionScoreModule;
import org.elasticsearch.index.search.stats.StatsGroupsParseElement;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.IndexStoreModule;
import org.elasticsearch.indices.*;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.monitor.MonitorModule;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.plugins.IndexPluginsModule;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationModule;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.facet.FacetModule;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.highlight.HighlightModule;
import org.elasticsearch.search.internal.*;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.search.suggest.SuggestModule;
import org.elasticsearch.search.suggest.SuggestPhase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.local.LocalTransport;


import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mateus on 29/08/15.
 */
public class SearchParse {

    private static long i = 0l;

    private static long incrementAndGet() {
        return ++i;
    }

    public static SearchResponse main(SearchRequest searchRequest, IndexSearcher searcher, BigArrays bigArrays, RestRequest request, TopDocs topDocs, ScoreDoc[] scoreDocs, Injector injector, final Client client, ESLogger logger) {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("path.home", "/tmp/")
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();


        String s = settings.get(IndexMetaData.SETTING_VERSION_CREATED);
        logger.info(new String(request.content().toBytes()), s);
        final InternalNode node = new InternalNode(settings, false);
        final Index index = new Index("index");
        ThreadPool threadPool = new ThreadPool("active");
        Engine.Searcher eSearcher = new Engine.Searcher("index", searcher);

        ShardSearchTransportRequest shardSearchTransportRequest;
        //SearchRequest searchRequest, ShardRouting shardRouting, int numberOfShards, boolean useSlowScroll, String[] filteringAliases, long nowInMillis
        shardSearchTransportRequest = new ShardSearchTransportRequest(searchRequest, new ImmutableShardRouting("index", 1, "1", true, ShardRoutingState.INITIALIZING, 1), 1, false, new String[]{}, new Date().getTime());
        SearchShardTarget searchShardTarget = new SearchShardTarget("local", "index", 1);
        IndicesService indexServices = injector.getInstance(IndicesService.class);
        indexServices.removeIndex("agg", "test");
        IndexService indexService = indexServices.createIndex("agg", settings, "test");//prepareIndex("teste_1", settings, injector);
            SearchContext context = new DefaultSearchContext(
                1l,
                shardSearchTransportRequest,
                searchShardTarget,
                eSearcher,
                indexService,
                null,
                null,
                new CacheRecycler(settings),
                new PageCacheRecycler(settings, threadPool),
                bigArrays,
                new Counter() {
                    long i;

                    @Override
                    public long addAndGet(long l) {
                        this.i += l;
                        return i;
                    }

                    @Override
                    public long get() {
                        return i;
                    }
                }
        );
        //indexService.index();
        SearchContext.setCurrent(context);



        Map<String, SearchParseElement> elementParsers = new HashMap<>();
        elementParsers.putAll(node.injector().getInstance(DfsPhase.class).parseElements());
        elementParsers.putAll(node.injector().getInstance(QueryPhase.class).parseElements());
        elementParsers.putAll(node.injector().getInstance(FetchPhase.class).parseElements());
        elementParsers.putAll(node.injector().getInstance(AggregationPhase.class).parseElements());
        elementParsers.put("stats", new StatsGroupsParseElement());



        parseSource(context, request.content(), ImmutableMap.copyOf(elementParsers));

        context.queryResult().topDocs(topDocs);
        int[] ids = new int[scoreDocs.length];
        for (int j = 0; j < scoreDocs.length; j++) {
            scoreDocs[j].shardIndex = 0;
            ids[j] = scoreDocs[j].doc;
        }
        context.docIdsToLoad(ids, 0, ids.length);
        context.from(0).size(10);
        AtomicArray query = new AtomicArray<QuerySearchResult>(1);
        query.set(0, context.queryResult());
        AtomicArray fetch = new AtomicArray<>(1);
        fetch.set(0, context.fetchResult());

        FetchPhase fetchPhase = node.injector().getInstance(FetchPhase.class);
        fetchPhase.execute(context);
        logger.info("------Size:" + context.fieldNames().size(), "jhere");
        logger.info("------Docs:" + context.docIdsToLoad().length, "jhere");
        logger.info("------"+context.fetchResult().hits().hits().length, "jhere");
        injector.getInstance(SuggestPhase.class).execute(context);
        AggregationPhase aggregationPhase= injector.getInstance(AggregationPhase.class);
        aggregationPhase.preProcess(context);
        aggregationPhase.execute(context);
        InternalSearchResponse respose =
                new SearchPhaseControllerUtil(settings, null, null, null)
                        .merge(scoreDocs, query, fetch);
        logger.info(respose.toString(), s);
        return new SearchResponse(respose, "1", 1, 1, System.currentTimeMillis(), new ShardSearchFailure[]{});
    }

    private static void parseSource(SearchContext context, BytesReference source, ImmutableMap<String, SearchParseElement> elementParsers) throws SearchParseException {
        // nothing to parse...
        if (source == null || source.length() == 0) {
            return;
        }
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            XContentParser.Token token;
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("Expected START_OBJECT but got " + token.name() + " " + parser.currentName());
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    SearchParseElement element = elementParsers.get(fieldName);
                    if (element == null) {
                        throw new SearchParseException(context, "No parser for element [" + fieldName + "]");
                    }
                    element.parse(parser, context);
                } else {
                    if (token == null) {
                        throw new ElasticsearchParseException("End of query source reached but query is not complete.");
                    } else {
                        throw new ElasticsearchParseException("Expected field name but got " + token.name() + " \"" + parser.currentName() + "\"");
                    }
                }
            }
        } catch (Throwable e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable e1) {
                // ignore
            }
            throw new SearchParseException(context, "Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
    static class SearchPhaseControllerUtil extends SearchPhaseController {

        public SearchPhaseControllerUtil(Settings settings, BigArrays bigArrays, ScriptService scriptService, CacheRecycler cacheRecycler) {
            //Settings settings, CacheRecycler cacheRecycler, BigArrays bigArrays, ScriptService scriptService
            super(settings, cacheRecycler, bigArrays, scriptService);
        }
    }
}


