import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.node.NodeBuilder.*;

// Elasticsearch 1.7 and 2.3 (current)
//  ./elasticsearch --cluster.name elasticsearch_sdakhani --node.name SpireonNode
public class ElasticJavaAPI{

    static String index = "my_deezer";
    static String type = "song";
    static String id = "11";

    public static Client connectToExistingCuster(){

        //Uses Transport Client
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "elasticsearch_sdakhani").build();


        Client client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        return client;

    }

    public static Client createAnEmbededode(){

        //Uses Node Client
        Node node = nodeBuilder().clusterName("elasticsearch_sdakhani").node();
        Client client = node.client();
        return client;
        //node.close();
    }

    public static void closeClient(Client client){
        client.close();
    }

    public static void indexAPI(Client client){

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("title","Fake");
        json.put("artist","DanceAndMight");
        json.put("year",1990);

        IndexResponse response = client.prepareIndex(index,type,id)
                .setSource(json)
                .execute()
                .actionGet();

        System.out.println(response.isCreated());

    }

    public static void getAPI(Client client){

        GetResponse response = client.prepareGet(index,type,id)
                .execute()
                .actionGet();

        System.out.println(response.getSourceAsMap());
        System.out.println(response.getSourceAsMap().get("title"));
        System.out.println(response.getSourceAsMap().get("artist"));
        System.out.println(response.getSourceAsMap().get("year"));
    }

    public static void updateAPI(Client client) {

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("title","Roses");

        try {
            UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                    .doc(json);
            UpdateResponse response =client.update(updateRequest).get();
            System.out.println(response.getVersion());
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void deleteAPI(Client client){

        DeleteResponse response = client.prepareDelete(index,type,id)
                .execute()
                .actionGet();
        System.out.println(response.isFound());
    }

    public static void main(String[] args) {

        Client client = connectToExistingCuster();
        indexAPI(client);
        getAPI(client);
        //updateAPI(client);
        //deleteAPI(client);
        closeClient(client);

    }
}
