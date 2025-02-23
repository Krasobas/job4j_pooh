package ru.job4j.pooh;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class TopicSchema implements Schema {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();
    private final BlockingQueue<String> batch = new LinkedBlockingQueue<>();

    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.computeIfAbsent(receiver.name(), l -> new CopyOnWriteArrayList<>())
            .add(receiver);
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.computeIfAbsent(message.name(), q -> new LinkedBlockingQueue<>())
            .add(message.text());
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            for (var queueKey : receivers.keySet()) {
                var queue = data.getOrDefault(queueKey, new LinkedBlockingQueue<>());
                queue.drainTo(batch);
                var receiversByQueue = receivers.get(queueKey);
                var it = receiversByQueue.iterator();
                while (it.hasNext()) {
                    var receiver = it.next();
                    batch.forEach(receiver::receive);
                    if (!it.hasNext()) {
                        batch.clear();
                        queue.drainTo(batch);
                        it = receiversByQueue.iterator();
                    }
                }

            }
            condition.off();
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
