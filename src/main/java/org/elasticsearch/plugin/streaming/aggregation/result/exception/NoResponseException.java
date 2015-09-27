package org.elasticsearch.plugin.streaming.aggregation.result.exception;

/**
 * Created by mateus on 27/09/15.
 */
public class NoResponseException extends  RuntimeException {
    public  NoResponseException(){
        super("There is no reponse for this aggregation!");
    }
}
