package org.apache.jackrabbit.oak.plusing.index.es.index;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.function.Predicate;

/**
 * Created by ieb on 20/05/2016.
 * Drains a queue containing futures as they are done. Does not assume they will be done in order.
 */
public class DrainingFutureQueue<T extends Future> extends ArrayBlockingQueue<T> implements Runnable {

    private final Callback callback;
    private boolean running;

    public DrainingFutureQueue(int size, Callback<T> cb) {
        super(size);
        this.callback = cb;
        running = true;
    }



    @Override
    public void run() {
        while(running) {
            if (size() == 0) {
                Thread.yield();
            } else {
                removeIf(new Predicate<T>() {
                    @Override
                    public boolean test(T indexResponseActionFuture) {
                        if (indexResponseActionFuture.isDone() || indexResponseActionFuture.isCancelled()) {
                            callback.done(indexResponseActionFuture);
                            return true;
                        }
                        ;
                        return false;
                    }
                });
            }

        }

    }

    public void stop() {
        running = false;
    }

    public interface Callback<T> {
        void done(T indexResponseActionFuture);
    }
}
