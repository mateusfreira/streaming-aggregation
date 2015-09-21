package org.elasticsearch.plugin.streaming.aggregation.result;



import java.util.ArrayList;
import java.util.List;

/**
 * Created by mateus on 21/09/15.
 */

public class MemoryResultStorage {
    private List<String> results = new ArrayList<String>();

    public void addResult(String result){
        this.results.add(result);
    }

    public void clearResults(){
        results.clear();
    }

    public List<String> results(){
        return results;
    }
}
