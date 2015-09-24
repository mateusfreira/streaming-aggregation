package org.elasticsearch.plugin.streaming.aggregation;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.plugin.streaming.aggregation.result.MemoryResultStorage;

public class StreamingAggregationModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(MemoryResultStorage.class).asEagerSingleton();
        bind(StreamingAggregationRestHandler.class).asEagerSingleton();
    }
}
