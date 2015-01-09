package bootcamp;

import java.util.function.Consumer;

public class FailSafeConsumer<T> implements Consumer<T> {

    private final Consumer<T> consumer;

    public FailSafeConsumer(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void accept(T t) {
        try {
            consumer.accept(t);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
