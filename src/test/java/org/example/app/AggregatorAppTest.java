package org.example.app;

import org.example.model.Agg;
import org.example.model.Result;
import org.example.model.RowBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregatorAppTest {

    private static Agg agg(long imp, long clk, double spend, long conv) {
        Agg a = new Agg();
        a.add(imp, clk, spend, conv);
        return a;
    }

    @Test
    void workerLoopAggregatesBatches() throws Exception {
        BlockingQueue<RowBatch> q = new ArrayBlockingQueue<>(2);
        Map<String, Agg> map = new HashMap<>();

        Thread t = new Thread(() -> AggregatorApp.workerLoop(q, map));
        t.start();

        RowBatch b = new RowBatch(3);
        b.add("c1", 10, 1, 2.0, 0);
        b.add("c1", 5, 1, 1.0, 1);
        b.add("c2", 7, 0, 3.5, 0);

        q.put(b);
        q.put(RowBatch.getPOISON());
        t.join(2000);

        Agg a1 = map.get("c1");
        assertNotNull(a1);
        assertEquals(15, a1.getImpressions());
        assertEquals(2, a1.getClicks());
        assertEquals(3.0, a1.getSpend(), 0.0001);
        assertEquals(1, a1.getConversions());

        Agg a2 = map.get("c2");
        assertNotNull(a2);
        assertEquals(7, a2.getImpressions());
        assertEquals(0, a2.getClicks());
        assertEquals(3.5, a2.getSpend(), 0.0001);
        assertEquals(0, a2.getConversions());
    }

    @Test
    void top10ByCtrReturnsTop10Descending() {
        @SuppressWarnings("unchecked")
        Map<String, Agg>[] shardMaps = new Map[2];
        shardMaps[0] = new HashMap<>();
        shardMaps[1] = new HashMap<>();

        for (int i = 1; i <= 12; i++) {
            String id = "c" + i;
            shardMaps[i % 2].put(id, agg(100, i, 0.0, 0));
        }

        List<Result> results = AggregatorApp.top10ByCtr(shardMaps);
        assertEquals(10, results.size());
        assertEquals("c12", results.get(0).getCampaignId());
        assertEquals("c3", results.get(9).getCampaignId());

        for (int i = 1; i < results.size(); i++) {
            assertTrue(results.get(i - 1).getCtr() >= results.get(i).getCtr());
        }
    }

    @Test
    void top10ByLowestCpaSkipsZeroConversionsAndSortsAscending() {
        @SuppressWarnings("unchecked")
        Map<String, Agg>[] shardMaps = new Map[2];
        shardMaps[0] = new HashMap<>();
        shardMaps[1] = new HashMap<>();

        shardMaps[0].put("czero", agg(100, 1, 1.0, 0));
        for (int i = 1; i <= 12; i++) {
            String id = "c" + i;
            shardMaps[i % 2].put(id, agg(100, 1, i, 1));
        }

        List<Result> results = AggregatorApp.top10ByLowestCpa(shardMaps);
        assertEquals(10, results.size());
        assertEquals("c1", results.get(0).getCampaignId());
        assertEquals("c10", results.get(9).getCampaignId());
        assertFalse(results.stream().anyMatch(r -> r.getCampaignId().equals("czero")));

        for (int i = 1; i < results.size(); i++) {
            assertTrue(results.get(i - 1).getCpa() <= results.get(i).getCpa());
        }
    }

    @Test
    void writeCsvFormatsValues(@TempDir Path tempDir) throws Exception {
        List<Result> rows = new ArrayList<>();
        rows.add(new Result("c1", agg(10, 2, 3.1, 0)));
        rows.add(new Result("c2", agg(20, 10, 5.0, 2)));

        Path file = tempDir.resolve("out.csv");
        AggregatorApp.writeCsv(file, rows);

        List<String> lines = Files.readAllLines(file);
        assertEquals(3, lines.size());
        assertEquals("campaign_id,total_impressions,total_clicks,total_spend,total_conversions,CTR,CPA", lines.get(0));
        assertEquals("c1,10,2,3.10,0,0.2000000,", lines.get(1));
        assertEquals("c2,20,10,5.00,2,0.5000000,2.50", lines.get(2));
    }

    @Test
    void testAggregationAndTop10() throws Exception {
        String csv = """
                campaign_id,date,impressions,clicks,spend,conversions
                CMP025,2025-04-18,3653,60,64.29,2
                CMP020,2025-05-03,24465,764,1394.62,42
                CMP019,2025-02-05,7214,236,135.93,21
                CMP043,2025-06-10,1074,34,56.89,1
                """;

        Path tmp = Files.createTempFile("ad_data_test", ".csv");
        Files.writeString(tmp, csv, StandardCharsets.UTF_8);

        int shards = 2;
        int batchSize = 2;

        @SuppressWarnings("unchecked")
        BlockingQueue<RowBatch>[] queues = new BlockingQueue[shards];
        for (int i = 0; i < shards; i++) queues[i] = new ArrayBlockingQueue<>(10);

        @SuppressWarnings("unchecked")
        Map<String, Agg>[] shardMaps = new Map[shards];
        for (int i = 0; i < shards; i++) shardMaps[i] = new HashMap<>();

        ExecutorService pool = Executors.newFixedThreadPool(shards);
        for (int i = 0; i < shards; i++) {
            int shard = i;
            pool.submit(() -> AggregatorApp.workerLoop(queues[shard], shardMaps[shard]));
        }

        AggregatorApp.parseCsvStreaming(tmp, queues, shards, batchSize);

        for (int i = 0; i < shards; i++) queues[i].put(RowBatch.getPOISON());

        pool.shutdown();
        assertTrue(pool.awaitTermination(1, TimeUnit.MINUTES));

        // Build top lists
        List<Result> topCtr = AggregatorApp.top10ByCtr(shardMaps);
        List<Result> topCpa = AggregatorApp.top10ByLowestCpa(shardMaps);

        // Expected CTR max is CMP019: 236/7214 ~ 0.0327
        assertEquals("CMP019", topCtr.get(0).getCampaignId());

        // Lowest CPA among provided:
        // CMP019: 135.93/21 ~ 6.47
        // CMP025: 64.29/2  ~ 32.15
        // CMP020: 1394.62/42 ~ 33.21
        // CMP043: 56.89/1 ~ 56.89
        assertEquals("CMP043", topCpa.getLast().getCampaignId());

        // Ensure conversions=0 excluded: add a row with conversions=0 and verify not present in topCpa
        // (not needed here; separate test below)
    }

    @Test
    void testZeroConversionsExcludedFromTopCpa() throws Exception {
        String csv = """
                campaign_id,date,impressions,clicks,spend,conversions
                CMP001,2025-01-01,100,10,50.00,0
                CMP002,2025-01-01,100,10,60.00,3
                """;
        Path tmp = Files.createTempFile("ad_data_test2", ".csv");
        Files.writeString(tmp, csv, StandardCharsets.UTF_8);

        int shards = 1;
        int batchSize = 10;

        @SuppressWarnings("unchecked")
        BlockingQueue<RowBatch>[] queues = new BlockingQueue[shards];
        queues[0] = new ArrayBlockingQueue<>(10);

        @SuppressWarnings("unchecked")
        Map<String, Agg>[] shardMaps = new Map[shards];
        shardMaps[0] = new HashMap<>();

        ExecutorService pool = Executors.newFixedThreadPool(1);
        pool.submit(() -> AggregatorApp.workerLoop(queues[0], shardMaps[0]));

        AggregatorApp.parseCsvStreaming(tmp, queues, shards, batchSize);
        queues[0].put(RowBatch.getPOISON());

        pool.shutdown();
        assertTrue(pool.awaitTermination(1, TimeUnit.MINUTES));

        List<Result> topCpa = AggregatorApp.top10ByLowestCpa(shardMaps);
        List<String> ids = topCpa.stream().map(r -> r.getCampaignId()).collect(Collectors.toList());

        assertTrue(ids.contains("CMP002"));
        assertFalse(ids.contains("CMP001")); // conversions=0 excluded
    }
}
