package org.elasticsearch.plugin.streaming.aggregation;

import org.elasticsearch.common.inject.AbstractModule;

public class StreamingAggregationModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(StreamingAggregationRestHandler.class).asEagerSingleton();
    }
}