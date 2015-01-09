import bootcamp.ElasticSearchWriter;
import bootcamp.EventStoreWriter;
import bootcamp.WebSocketTweetEmitter;
import bootcamp.TweetProducer;

import java.io.IOException;

import static com.google.common.collect.Lists.newArrayList;

public class BootCampTwitterCase {

    public static void main(String[] args) throws InterruptedException, IOException {
        new TweetProducer(newArrayList(
                new EventStoreWriter(),
                new ElasticSearchWriter(),
                new WebSocketTweetEmitter()
        )).run();
    }

}
