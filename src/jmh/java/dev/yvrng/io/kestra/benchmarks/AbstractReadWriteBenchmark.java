package dev.yvrng.io.kestra.benchmarks;

import static io.kestra.core.utils.Rethrow.throwConsumer;

import io.kestra.core.serializers.JacksonMapper;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import reactor.core.publisher.Flux;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(1)
@State(value = Scope.Benchmark)
public abstract class AbstractReadWriteBenchmark {

    @Param({"1000", "10000", "100000"})
    protected int count;

    @Param({"x-small", "small", "medium"}) // TODO add "large" and "x-large"
    protected String size;

    private Path tempDirectory;

    @Setup
    public final void createTempDirectory() throws IOException {
        tempDirectory = Files.createTempDirectory("io-kestra-benchmarks");
    }

    @TearDown
    public final void deleteTempDirectory() throws IOException {
        if (tempDirectory != null && Files.exists(tempDirectory)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(tempDirectory)) {
                stream.forEach(throwConsumer(Files::delete));
            }
            Files.delete(tempDirectory);
        }
    }

    public Path createTempFile() throws IOException {
        return Files.createTempFile(tempDirectory, "dataset-%s-%d-".formatted(size, count), ".ion");
    }

    public Flux<?> createSampleFlux() throws IOException {
        final Object record = createSampleRecord();
        return Flux.generate(sink -> sink.next(record));
    }

    private Object createSampleRecord() throws IOException {
        final var recordFileUrl = AbstractReadWriteBenchmark.class.getResource("/samples/%s.json".formatted(size));
        return JacksonMapper.ofJson().readValue(recordFileUrl, Object.class);
    }
}
