package bootcamp;

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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TweetProducer {

    private final List<Consumer<String>> tweetConsumers;

    public TweetProducer(List<Consumer<String>> tweetConsumers) {
        this.tweetConsumers = tweetConsumers
                .stream()
                .map((Function<Consumer<String>, Consumer<String>>) FailSafeConsumer::new)
                .collect(Collectors.toList());
    }

    public void run() {
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

        System.out.println("Connected to twitter, now reading tweets...");

        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            try {
                String msg = msgQueue.take();
                this.tweetConsumers.forEach(c -> c.accept(msg));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        hosebirdClient.stop();
    }

}
