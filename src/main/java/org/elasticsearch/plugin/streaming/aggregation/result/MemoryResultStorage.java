package org.elasticsearch.plugin.streaming.aggregation.result;



import org.elasticsearch.rest.RestRequest;

import java.util.HashMap;
import java.util.List;

/**
 * Created by mateus on 21/09/15.
 */

public class MemoryResultStorage {
    private HashMap<String, org.elasticsearch.rest.RestRequest> searchs  = new HashMap();

    public void put(String id, RestRequest request){
        this.searchs.put(id, request);
    }

    public RestRequest get(String id){
        return this.searchs.get(id);
    }

    public void clearResults(){
        searchs.clear();
    }
}
