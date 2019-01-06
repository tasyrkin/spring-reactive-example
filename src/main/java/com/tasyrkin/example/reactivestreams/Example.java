package com.tasyrkin.example.reactivestreams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Example {

    private static final Logger LOG = LogManager.getLogger(Example.class);

    public static void main(String[] args) throws InterruptedException {
        final IntPublisher publisher = new IntPublisher();
        final IntSubscriber subscriber = new IntSubscriber();

        publisher.subscribe(subscriber);


        Thread.sleep(10000);
        publisher.shutdown();
    }

    static class IntSubscriber implements Subscriber<Integer> {

        private static final Logger LOG = LogManager.getLogger(IntSubscriber.class);

        private long accumulator = 0;

        @Override
        public void onSubscribe(final Subscription subscription) {
            LOG.debug(String.format("subscribing with subscription = %s", subscription));
            subscription.request(10);
        }

        @Override
        public void onNext(final Integer num) {
            accumulator = accumulator + num;
        }

        @Override
        public void onError(final Throwable throwable) {
            LOG.error("Received an error", throwable);
        }

        @Override
        public void onComplete() {
            LOG.debug(String.format("Received a stream completion, accumulator = %s", accumulator));
        }
    }

    static class IntPublisher implements Publisher<Integer> {

        private final Random random = new Random(System.currentTimeMillis());

        private final ExecutorService executorService = Executors.newFixedThreadPool(10, new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                return new Thread(r, "IntPublisher Thread");
            }
        });

        @Override
        public void subscribe(final Subscriber<? super Integer> subscriber) {

            final IntSubscription subscription = new IntSubscription(this, subscriber);

            subscriber.onSubscribe(subscription);
        }

        void request(final IntSubscription subscription, final long n) {
            executorService.submit(new RequestProcessor(subscription, n));
        }

        void cancel(final IntSubscription subscription) {

        }

        int getNext() {
            return random.nextInt();
        }

        void shutdown() {
            executorService.shutdown();
        }
    }

    static class RequestProcessor implements Runnable {

        private final IntSubscription subscription;
        private final long n;

        public RequestProcessor(final IntSubscription subscription, long n) {
            this.subscription = subscription;
            this.n = n;
        }

        @Override
        public void run() {
            for (int num = 0; num < n; num++) {
                subscription.getSubscriber().onNext(subscription.getPublisher().getNext());
            }
        }
    }

    static class IntSubscription implements Subscription {

        private final IntPublisher publisher;
        private final Subscriber<? super Integer> subscriber;

        IntSubscription(final IntPublisher publisher, final Subscriber<? super Integer> subscriber) {
            this.publisher = publisher;
            this.subscriber = subscriber;
        }

        @Override
        public void request(final long n) {
            publisher.request(this, n);
        }

        @Override
        public void cancel() {
            publisher.cancel(this);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final IntSubscription that = (IntSubscription) o;
            return publisher.equals(that.publisher) &&
                    subscriber.equals(that.subscriber);
        }

        @Override
        public int hashCode() {
            return Objects.hash(publisher, subscriber);
        }

        public IntPublisher getPublisher() {
            return publisher;
        }

        public Subscriber<? super Integer> getSubscriber() {
            return subscriber;
        }
    }

}
