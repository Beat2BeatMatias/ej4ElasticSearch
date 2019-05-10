import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

import static spark.Spark.*;

public class Application {



    public static void main(String[] args) throws IOException {
        port(8080);

        ItemService.makeConnection();
        System.out.println("Insertando un nuevo item");
        Item itemInitial = new Item("MLA","Pelota", "MLA1055",1000,"ARS");
        ItemService.insertItem(itemInitial);


        get("/items", (request, response) -> {
            response.type("application/json");
            ArrayList arrayListItems=ItemService.getAllItems();
            JsonElement gson = new Gson().toJsonTree(arrayListItems);
            return new Gson().toJson(new StandarResponse(StatusResponse.SUCCESS, gson));
        });


        get("/item/:item_id", (request, response) -> {
            response.type("application/json");
            String itemId = request.params("item_id");
            if (ItemService.getItemById(itemId)!=null) {
                JsonElement gson = new Gson().toJsonTree(ItemService.getItemById(itemId));
                return new Gson().toJson(new StandarResponse(StatusResponse.SUCCESS, gson));
            }else {
                return new Gson().toJson(new StandarResponse(StatusResponse.ERROR, "No existe el elemento solicitado"));
            }
        });

        put("/item", (request, response) -> {
            response.type("application/json");
            Item item = new Gson().fromJson(request.body(), Item.class);
            if(ItemService.getItemById(item.getId())!=null) {
                Item itemActualizado = ItemService.updateItemById(item);
                JsonElement gson = new Gson().toJsonTree(itemActualizado);
                return new Gson().toJson(new StandarResponse(StatusResponse.SUCCESS, gson));
            }else{
                return new Gson().toJson(new StandarResponse(StatusResponse.ERROR,"No existe el item a modificar..."));
            }
        });


        post("/item", (request, response) -> {
            response.type("application/json");
            Item item  = new Gson().fromJson(request.body(), Item.class);
            Item itemAgregado=ItemService.insertItem(item);
            JsonElement gson=new Gson().toJsonTree(itemAgregado);
            return new Gson().toJson(new StandarResponse(StatusResponse.SUCCESS,gson));
        });

        delete("/item/:item_id", (request, response) -> {
            response.type("application/json");
            String itemId = request.params("item_id");
            if(ItemService.getItemById(itemId)!=null) {
                ItemService.deletePersonById(itemId);
                return new Gson().toJson(new StandarResponse(StatusResponse.SUCCESS));
            }
            else {
                return new Gson().toJson(new StandarResponse(StatusResponse.ERROR));
            }
        });
    }
}
