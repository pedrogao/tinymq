package github.io.pedrogao.dqv2;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(10)
public class DiskQueueBenchmark {

    private DiskQueue queue;

    public DiskQueueBenchmark() {
        File file = null;
        try {
            file = File.createTempFile("dqv2", ".q");
            queue = new DiskQueue(file, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void offer() {
        queue.offer("Hello Queue".getBytes());
    }

    @Benchmark
    public void offerAndPoll() {
        queue.offer("Hello Queue".getBytes());
        queue.poll();
    }

    @Threads(10)
    @Benchmark
    public void concurrentOffer() {
        queue.offer("Hello Queue".getBytes());
    }

    public static void main(String[] args) throws Exception {
        Options opts = new OptionsBuilder()
                .include(DiskQueueBenchmark.class.getSimpleName())
                .build();
        new Runner(opts).run();
    }
}
