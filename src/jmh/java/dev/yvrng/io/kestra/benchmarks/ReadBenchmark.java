package dev.yvrng.io.kestra.benchmarks;

import io.kestra.core.serializers.FileSerde;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

public class ReadBenchmark extends AbstractReadWriteBenchmark {

    private Flux<?> inputFlux;
    private Path inputFile;

    @Setup
    public void setup() throws IOException {
        inputFlux = createSampleFlux();

        // write file to be read
        inputFile = createTempFile();
        FileSerde.writeAll(Files.newOutputStream(inputFile), inputFlux.take(count)).block();
    }

    @Benchmark
    public void readAll_withBufferedReader(Blackhole bh) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            Flux.create(FileSerde.reader(reader)).doOnNext(bh::consume).count().block();
        }
    }

    @Benchmark
    public void readAll_withMappingIterator(Blackhole bh) throws IOException {
        FileSerde.readAll(Files.newInputStream(inputFile)).doOnNext(bh::consume).count().block();
    }
}
