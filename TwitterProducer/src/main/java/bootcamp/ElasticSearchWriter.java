package bootcamp;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Consumer;

public class ElasticSearchWriter implements Consumer<String> {

    private final CloseableHttpClient httpClient = HttpClients.createDefault();

    @Override
    public void accept(String msg) {
        try {
            String guid = UUID.randomUUID().toString();
            HttpPut httpPut = new HttpPut("http://localhost:9200/twitter/tweet/" + guid);
            httpPut.setEntity(new StringEntity(msg));
            CloseableHttpResponse response = httpClient.execute(httpPut);
            response.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
