package org.elasticsearch.plugin.streaming.aggregation.test;

import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Counter;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.query.parser.QueryParserCacheModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.search.stats.StatsGroupsParseElement;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.*;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.internal.*;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;

/**
 * Created by mateus on 29/08/15.
 */
public class SearchParse {

    private static long i = 0l;
    private static long incrementAndGet(){
        return ++i;
    }

    public static void main(SearchRequest searchRequest, Settings settings, IndexSearcher searcher, BigArrays bigArrays,  RestRequest request, TopDocs topDocs, ScoreDoc[] scoreDocs){
        InternalNode node = new InternalNode(settings, false);
        Index index = new Index("index");
        ThreadPool threadPool = new ThreadPool("active");

        Engine.Searcher eSearcher =  new Engine.Searcher("index", searcher);
        /*
        ModulesBuilder modulesBuilder = new ModulesBuilder();
        Injector injector = modulesBuilder.add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, node.injector().getInstance(IndicesAnalysisService.class)),
                new SimilarityModule(settings),
                new QueryParserCacheModule(settings)
        )
                .createChildInjector(node.injector());
        */
        //searchRequest
        ShardSearchTransportRequest shardSearchTransportRequest;
                //SearchRequest searchRequest, ShardRouting shardRouting, int numberOfShards, boolean useSlowScroll, String[] filteringAliases, long nowInMillis
        shardSearchTransportRequest = new ShardSearchTransportRequest(searchRequest, new ImmutableShardRouting("index", 1,"1",true, ShardRoutingState.INITIALIZING, 1), 1, false, new String[]{}, new Date().getTime());
        SearchShardTarget searchShardTarget = new SearchShardTarget("local", "index", 1);
        IndexService indexService = null;//injector.getInstance(IndexService.class);
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
        SearchContext.setCurrent(context);

        Map<String, SearchParseElement> elementParsers = new HashMap<>();
        elementParsers.putAll(node.injector().getInstance(DfsPhase.class).parseElements());
        elementParsers.putAll(node.injector().getInstance(QueryPhase.class).parseElements());
        elementParsers.putAll(node.injector().getInstance(FetchPhase.class).parseElements());
        elementParsers.put("stats", new StatsGroupsParseElement());


        parseSource(context, request.content(), ImmutableMap.copyOf(elementParsers));
        //AggregationPhase aggregationPhase= injector.getInstance(AggregationPhase.class);
        //aggregationPhase.preProcess(context);
        //aggregationPhase.execute(context);
        AtomicArray query = new AtomicArray<>(1);
        query.set(0, context.queryResult());
        AtomicArray fetch = new AtomicArray<>(1);
        fetch.set(0, context.fetchResult());
        //context.fetchResult().hits();
        context.queryResult().topDocs(topDocs);
        InternalSearchResponse respose =
                new SearchPhaseControllerUtil(settings, null, null, null)
                        .merge(scoreDocs, query, fetch);
        respose.hits();

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
                throw new ElasticsearchParseException("failed to parse search source. source must be an object, but found [{}] instead", null);
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    SearchParseElement element = elementParsers.get(fieldName);
                    if (element == null) {
                        throw new SearchParseException(context, "failed to parse search source. unknown search element [" + fieldName + "]", null);
                    }
                    element.parse(parser, context);
                } else {
                    if (token == null) {
                        throw new ElasticsearchParseException("failed to parse search source. end of query source reached but query is not complete.");
                    } else {
                        throw new ElasticsearchParseException("failed to parse search source. expected field name but got [{}]", null);
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
            throw new SearchParseException(context, "failed to parse search source [" + sSource + "]", null);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}

class SearchPhaseControllerUtil extends SearchPhaseController {

    public SearchPhaseControllerUtil(Settings settings, BigArrays bigArrays, ScriptService scriptService, CacheRecycler cacheRecycler) {
        //Settings settings, CacheRecycler cacheRecycler, BigArrays bigArrays, ScriptService scriptService
        super(settings, cacheRecycler, bigArrays, scriptService);
    }
}
