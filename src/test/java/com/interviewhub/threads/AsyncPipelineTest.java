package com.interviewhub.threads;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;


public class AsyncPipelineTest {

    @Test
    public void testProducerConsumerQueue() throws InterruptedException {
        TaskQueue queue = new TaskQueue(2); // Small capacity to force producer wait()
        List<InterviewTask> tasks = List.of(
                new InterviewTask(1, InterviewTask.TaskType.REQUEST_PROCESSING, "Task 1"),
                new InterviewTask(2, InterviewTask.TaskType.NOTIFICATION_DISPATCH, "Task 2"),
                new InterviewTask(3, InterviewTask.TaskType.EVALUATION_REPORT_GENERATION, "Task 3")
        );

        TaskProducer producer = new TaskProducer(queue, tasks);
        TaskConsumer consumer = new TaskConsumer(queue);

        Thread producerThread = new Thread(producer, "TestProducer");
        Thread consumerThread = new Thread(consumer, "TestConsumer");

        consumerThread.start();
        producerThread.start();

        producerThread.join();
        Thread.sleep(400);
        queue.signalShutdown();
        consumerThread.join();

        assertEquals(3, consumer.getProcessedCount(), "Consumer must process all 3 produced tasks");
        assertTrue(queue.isEmpty(), "Queue should be empty after consumption");
    }

    @Test
    public void testExecutorServiceFixedThreadPool() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        Callable<Integer> calc1 = () -> {
            Thread.sleep(50);
            return 85 + 90;
        };

        Callable<Integer> calc2 = () -> {
            Thread.sleep(50);
            return 70 + 80;
        };

        Future<Integer> f1 = executor.submit(calc1);
        Future<Integer> f2 = executor.submit(calc2);

        assertEquals(175, f1.get());
        assertEquals(150, f2.get());

        executor.shutdown();
        assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        assertTrue(executor.isShutdown());
    }
}
