package org.example.app;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.example.model.Agg;
import org.example.model.Args;
import org.example.model.BatchDispatchRowProcessor;
import org.example.model.Result;
import org.example.model.RowBatch;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AggregatorApp {

    public static void main(String[] args) throws Exception {
        Args a = Args.parse(args);

        long t0 = System.nanoTime();

        Files.createDirectories(a.getOutputDir());

        final int shards = Math.max(1, a.getThreads());
        final int batchSize = a.getBatchSize();

        @SuppressWarnings("unchecked")
        BlockingQueue<RowBatch>[] queues = new BlockingQueue[shards];
        for (int i = 0; i < shards; i++) {
            queues[i] = new ArrayBlockingQueue<>(a.getQueueCapacity());
        }

        @SuppressWarnings("unchecked")
        Map<String, Agg>[] shardMaps = new Map[shards];
        for (int i = 0; i < shards; i++) {
            shardMaps[i] = new HashMap<>(1 << 16);
        }

        ExecutorService pool = Executors.newFixedThreadPool(shards);
        for (int i = 0; i < shards; i++) {
            final int shard = i;
            pool.submit(() -> workerLoop(queues[shard], shardMaps[shard]));
        }

        // // Producer: parse CSV streaming và dispatch batch vào shard queues
        parseCsvStreaming(a.getInput(), queues, shards, batchSize);

        // Send poison pills to stop workers
        for (int i = 0; i < shards; i++) {
            queues[i].put(RowBatch.getPOISON());
        }

        pool.shutdown();
        if (!pool.awaitTermination(60, TimeUnit.MINUTES)) {
            pool.shutdownNow();
            throw new RuntimeException("Workers timeout");
        }

        // Build top lists from shard maps (no need merge to 1 big map)
        List<Result> topCtr = top10ByCtr(shardMaps);
        List<Result> topCpa = top10ByLowestCpa(shardMaps);

        writeCsv(a.getOutputDir().resolve("top10_ctr.csv"), topCtr);
        writeCsv(a.getOutputDir().resolve("top10_cpa.csv"), topCpa);

        long t1 = System.nanoTime();
        double sec = (t1 - t0) / 1_000_000_000.0;

        long used = usedMemoryBytes();
        System.out.printf(
                Locale.US,
                "Done. Time: %.2fs, approx used memory: %.2f MB%n",
                sec, used / 1024.0 / 1024.0
        );
    }

    public static void workerLoop(BlockingQueue<RowBatch> q, Map<String, Agg> map) {
        try {
            while (true) {
                RowBatch b = q.take();
                if (b.isPoison()) break;

                for (int i = 0; i < b.getSize(); i++) {
                    String id = b.getCampaignId()[i];
                    Agg agg = map.get(id);
                    if (agg == null) {
                        agg = new Agg();
                        map.put(id, agg);
                    }
                    agg.add(b.getImpressions()[i], b.getClicks()[i], b.getSpend()[i], b.getConversions()[i]);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ======== Top-10 ========
    public static List<Result> top10ByCtr(Map<String, Agg>[] shardMaps) {
        PriorityQueue<Result> pq = new PriorityQueue<>(Comparator.comparingDouble(r -> r.getCtr())); // min-heap size 10

        for (Map<String, Agg>
                m : shardMaps) {
            for (var e : m.entrySet()) {
                Result r = new Result(e.getKey(), e.getValue());
                if (pq.size() < 10) pq.offer(r);
                else if (r.getCtr() > pq.peek().getCtr()) {
                    pq.poll();
                    pq.offer(r);
                }
            }
        }

        ArrayList<Result> out = new ArrayList<>(pq);
        out.sort((a, b) -> Double.compare(b.getCtr(), a.getCtr())); // desc CTR
        return out;
    }

    public static List<Result> top10ByLowestCpa(Map<String, Agg>[] shardMaps) {
        // max-heap by CPA => keep 10 smallest
        PriorityQueue<Result> pq = new PriorityQueue<>((a, b) -> Double.compare(b.getCpa(), a.getCpa()));

        for (Map<String, Agg> m : shardMaps) {
            for (var e : m.entrySet()) {
                Result r = new Result(e.getKey(), e.getValue());
                if (r.getCpa() == null) continue; // exclude conversions=0
                if (pq.size() < 10) pq.offer(r);
                else if (r.getCpa() < pq.peek().getCpa()) {
                    pq.poll();
                    pq.offer(r);
                }
            }
        }

        ArrayList<Result> out = new ArrayList<>(pq);
        out.sort(Comparator.comparingDouble(a -> a.getCpa())); // asc CPA
        return out;
    }

    // ======== CSV Writer ========
    public static void writeCsv(Path file, List<Result> rows) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            w.write("campaign_id,total_impressions,total_clicks,total_spend,total_conversions,CTR,CPA");
            w.newLine();

            for (Result r : rows) {
                String cpaStr = (r.getCpa() == null) ? "" : String.format(Locale.US, "%.2f", r.getCpa());

                w.write(r.getCampaignId());
                w.write(',');
                w.write(Long.toString(r.getTotalImpressions()));
                w.write(',');
                w.write(Long.toString(r.getTotalClicks()));
                w.write(',');
                w.write(String.format(Locale.US, "%.2f", r.getTotalSpend()));
                w.write(',');
                w.write(Long.toString(r.getTotalConversions()));
                w.write(',');
                w.write(String.format(Locale.US, "%.7f", r.getCtr()));
                w.write(',');
                w.write(cpaStr);
                w.newLine();
            }
        }
    }

    public static void parseCsvStreaming(Path input,
                                  BlockingQueue<RowBatch>[] queues,
                                  int shards,
                                  int batchSize) throws IOException {

        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true);
        settings.setSkipEmptyLines(true);
        settings.setLineSeparatorDetectionEnabled(true);
        settings.getFormat().setDelimiter(',');

        // Processor: custom RowProcessor + batch queue
        settings.setProcessor(new BatchDispatchRowProcessor(queues, shards, batchSize));

        try (InputStream in = new BufferedInputStream(Files.newInputStream(input), 16 * 1024 * 1024);
             Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            new CsvParser(settings).parse(reader); // streaming
        }
    }

    static long usedMemoryBytes() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }
}
