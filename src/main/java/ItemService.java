import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

public class ItemService {

    //The config parameters for the connection
    private static final String HOST = "localhost";
    private static final int PORT_ONE = 9200;
    private static final int PORT_TWO = 9201;
    private static final String SCHEME = "http";

    private static RestHighLevelClient restHighLevelClient;
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final String INDEX = "category";
    private static final String TYPE = "item";

    /**
     * Implemented Singleton pattern here
     * so that there is just one connection at a time.
     * @return RestHighLevelClient
     */
    static synchronized RestHighLevelClient makeConnection() {

        if(restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(HOST, PORT_ONE, SCHEME),
                            new HttpHost(HOST, PORT_TWO, SCHEME)));
        }

        return restHighLevelClient;
    }

    static synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }
    static Item insertItem(Item item){
        item.setId(UUID.randomUUID().toString());
        Map<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("id", item.getId());
        dataMap.put("siteId", item.getSiteId());
        dataMap.put("title", item.getTitle());
        dataMap.put("categoryId", item.getCategoryId());
        dataMap.put("price", item.getPrice());
        dataMap.put("currencyId", item.getCurrencyId());
        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, item.getId())
                .source(dataMap);
        try {
            IndexResponse response = restHighLevelClient.index(indexRequest);
        } catch(ElasticsearchException e) {
            e.getDetailedMessage();
        } catch (IOException ex){
            ex.getLocalizedMessage();
        }
        return item;
    }

    static Item getItemById(String id){
        GetRequest getItemRequest = new GetRequest(INDEX, TYPE, id);
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(getItemRequest);
        } catch (IOException e){
            e.getLocalizedMessage();
        }
        return getResponse != null ?
                objectMapper.convertValue(getResponse.getSourceAsMap(), Item.class) : null;
    }
    static ArrayList<Map> getAllItems(){
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse=null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ArrayList<Map> listaHits=new ArrayList<Map>();
        searchResponse.getHits().forEach(hit -> listaHits.add(hit.getSourceAsMap()));
       return listaHits;
    }

    static Item updateItemById(Item item){
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, item.getId())
                .fetchSource(true);    // Fetch Object after its update
        try {
            String personJson = objectMapper.writeValueAsString(item);
            updateRequest.doc(personJson, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
            return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Item.class);
        }catch (JsonProcessingException e){
            e.getMessage();
        } catch (IOException e){
            e.getLocalizedMessage();
        }
        System.out.println("Unable to update person");
        return null;
    }

    static void deletePersonById(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
        } catch (IOException e){
            e.getLocalizedMessage();
        }
    }
}
