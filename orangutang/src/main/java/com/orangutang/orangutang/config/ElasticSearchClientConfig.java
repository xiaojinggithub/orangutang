package com.orangutang.orangutang.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;

/**
 * elasticsearch Java High Level REST Client-->> ES 6+  is requerid
 */
public class ElasticSearchClientConfig {
    @Value("${elasticsearch.localhost}")
    private static String hostname;
    @Value("${elasticsearch.port}")
    private static int port;
    private static RestHighLevelClient client;

    public static RestHighLevelClient getElasticSearchClient(){
        if(client==null){
            try{
                RestHighLevelClient client = new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost(hostname, port, "http")/*,
                                new HttpHost("localhost", 9201, "http")*/));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return client;
    }

    public static void closeElasticSearchClient() throws Exception{
        if(client!=null){
            client.close();
        }
    }

}
