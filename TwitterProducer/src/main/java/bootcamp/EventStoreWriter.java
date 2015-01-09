package bootcamp;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.message.BasicHeader;

import javax.json.Json;
import javax.json.stream.JsonParser;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;

public class EventStoreWriter implements Consumer<String> {

    private final CloseableHttpClient httpClient;

    public EventStoreWriter(){
       CredentialsProvider credsProvider = new BasicCredentialsProvider();
       credsProvider.setCredentials(
               new AuthScope("localhost", 2113),
               new UsernamePasswordCredentials("admin", "changeit"));

       httpClient = HttpClients.custom()
               .setDefaultCredentialsProvider(credsProvider)
               .setRedirectStrategy(new LaxRedirectStrategy())
               .build();
   }

    @Override
    public void accept(String msg) {
        JsonParser jsonParser = Json.createParser(new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8)));

        String country = getCountry(jsonParser);
        if (!country.isEmpty()) {
            insertToEventStore(httpClient, msg, country);
        }
    }

    private static String getCountry(JsonParser jsonParser) {
        while(jsonParser.hasNext()) {
            JsonParser.Event event = jsonParser.next();
            if (event == JsonParser.Event.KEY_NAME && "country_code".equals(jsonParser.getString())) {
                JsonParser.Event country = jsonParser.next();
                if (country != JsonParser.Event.VALUE_NULL) {
                    return jsonParser.getString();
                }

                // We stop at first country_code, hacky but works for bootcamp
                break;
            }
        }
        return "";
    }

    private static void insertToEventStore(CloseableHttpClient httpClient, String msg, String country) {
        String guid = UUID.randomUUID().toString();
        country = country.replace(" ", "%20");
        HttpPost httpPost = new HttpPost("http://localhost:2113/streams/twitter-" + country + "/incoming/" + guid);
        httpPost.setHeader(new BasicHeader("Content-type", "application/json"));
        httpPost.setHeader(new BasicHeader("ES-EventType", "newtweet"));

        try {
            httpPost.setEntity(new StringEntity(msg));

            CloseableHttpResponse response = httpClient.execute(httpPost);
            response.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
