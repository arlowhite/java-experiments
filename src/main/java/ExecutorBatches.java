import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ExecutorBatches {

    static final Logger log = LogManager.getLogger();

    static class Batch implements Callable<Boolean> {

        private final List<Integer> list;

        public Batch(List<Integer> list) {
            this.list = list;
        }

        @Override
        public Boolean call() {
            log.info("Processing list: {}", list);
            log.info("daemon={}", Thread.currentThread().isDaemon());

            if (list.contains(42)) {
                log.warn("THROWING");
                throw new IllegalStateException("Found " + 42);
            }

            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }

    // FIXME not working
    static Thread.UncaughtExceptionHandler workerErrorHandler = (t, e) -> {
        log.error(t + " error: ", e);
    };

    static RejectedExecutionHandler rejectedExecutionHandler = (r, executor) -> {
        log.error("{} rejected {}", executor, r);
    };

    public static void main(String[] args) {

        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("CrudBatchWorker-%d")
                // FIXME not working?
                .uncaughtExceptionHandler(workerErrorHandler)
                .build();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(3, threadFactory);
        executor.setRejectedExecutionHandler(rejectedExecutionHandler);

        // Don't submit tasks while queue is greater than this
        final int maxQueueSize = 3;

        // submit batch once this many items are collected
        int maxBatchSize = 10;
        List<Integer> list = new ArrayList<>();

        // emulate iterating through work tasks
        for (int i = 0; i < 105; i++) {
            list.add(i);

            if (list.size() == maxBatchSize) {
                // submit this batch
                log.info("submitting batch {}", list);
                log.info("active={} tasks={} completed={} max={} queueSize={}",
                        executor.getActiveCount(), executor.getTaskCount(), executor.getCompletedTaskCount(),
                        executor.getMaximumPoolSize(), executor.getQueue().size());

                // don't want to race ahead
                final long start = System.currentTimeMillis();
                while (executor.getQueue().size() > maxQueueSize) {
                    // wait for queue to reduce
                    try {
//                        log.trace("waiting 50ms");
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                final long waited = System.currentTimeMillis() - start;
                log.trace("waited {}ms for CrudBatchWorker", waited);
                if (waited > 100) {
                    log.debug("waited {}ms for a free thread", waited);
                }

                executor.submit(new Batch(list));

                // reset list
                list = new ArrayList<>();
            }

        }

        if (!list.isEmpty()) {
            log.info("submitting remaining: {}", list);
            executor.submit(new Batch(list));
        }

        log.info("shutdown");
        executor.shutdown();
        try {
            executor.awaitTermination(20, TimeUnit.SECONDS);
            log.info("{} tasks done", executor.getCompletedTaskCount());
            if (executor.getTaskCount() != executor.getCompletedTaskCount()) {
                log.warn("tasks mismatch!");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void processBatch(List<Integer> list) {
        try {
            Thread.sleep(1_000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
