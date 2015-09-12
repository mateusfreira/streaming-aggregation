package org.elasticsearch.plugin.streaming.aggregation;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Collection;

public class StreamingAggregationPlugin extends AbstractPlugin {
    @Override public String name() {
        return "streaming.aggregation";
    }

    @Override public String description() {
        return "In test";
    }
  	@Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(StreamingAggregationModule.class);
        return modules;
    }    
}