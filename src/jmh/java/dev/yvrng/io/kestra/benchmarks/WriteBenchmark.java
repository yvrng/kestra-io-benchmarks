package dev.yvrng.io.kestra.benchmarks;

import static io.kestra.core.utils.Rethrow.throwConsumer;

import io.kestra.core.serializers.FileSerde;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

public class WriteBenchmark extends AbstractReadWriteBenchmark {

    private Flux<?> inputFlux;
    private Path outputFile;

    @Setup
    public void setup() throws IOException {
        inputFlux = createSampleFlux();
    }

    @Setup(Level.Invocation)
    public void init() throws IOException {
        outputFile = createTempFile();
    }

    @Benchmark
    public Long writeAll_withOutputStream(Blackhole bh) throws IOException {
        try (OutputStream out = Files.newOutputStream(outputFile)) {
            return inputFlux.take(count).doOnNext(throwConsumer(animal -> FileSerde.write(out, animal))).count().block();
        }
    }

    @Benchmark
    public Long writeAll_withSequenceWriter(Blackhole bh) throws IOException {
        return FileSerde.writeAll(Files.newOutputStream(outputFile), inputFlux.take(count)).block();
    }
}
