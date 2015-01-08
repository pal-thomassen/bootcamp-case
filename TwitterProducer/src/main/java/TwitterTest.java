import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
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

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterTest {

    public static void main(String[] args) throws InterruptedException, IOException {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("twitter", "api");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1("0jxNjsOFLtuc1Gd0U9Gwa1hwM", "QK4tKTfsTUl10XLOllyl4mH7L9cw6Xernut2lh4yEx8lJCADft", "140374649-Zx00P7LpKk8oYofoHQMflhdWYceSDjnrHpbEUqKs", "ScSaB13WDkLWbMGkhwotrARHMrPOBYFV99sa120PchwmK");

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        hosebirdClient.connect();

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                new AuthScope("localhost", 2113),
                new UsernamePasswordCredentials("admin", "changeit"));

        CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .setRedirectStrategy(new LaxRedirectStrategy())
                .build();

        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = msgQueue.take();


            String guid = UUID.randomUUID().toString();
            HttpPost httpPost = new HttpPost("http://localhost:2113/streams/twittertest/incoming/" + guid);
            httpPost.setHeader(new BasicHeader("Content-type", "application/json"));
            httpPost.setHeader(new BasicHeader("ES-EventType", "newtweet"));


            httpPost.setEntity(new StringEntity(msg));
            try {
                CloseableHttpResponse response = httpClient.execute(httpPost);
                response.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        System.out.println("After while");

        httpClient.close();

        hosebirdClient.stop();
    }
}
