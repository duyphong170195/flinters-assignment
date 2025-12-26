package org.example.model;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;
import org.example.util.ConvertUtil;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchDispatchRowProcessor implements RowProcessor {
    private final BlockingQueue<RowBatch>[] queues;
    private final int shards;
    private final int batchSize;

    // schema fixed: campaign_id,date,impressions,clicks,spend,conversions
    private static final int IDX_CAMPAIGN = 0;
    private static final int IDX_IMP = 2;
    private static final int IDX_CLICKS = 3;
    private static final int IDX_SPEND = 4;
    private static final int IDX_CONV = 5;

    private final RowBatch[] currentBatchPerShard;

    // optional: counting for debug
    private final AtomicInteger rowsSeen = new AtomicInteger();

    public BatchDispatchRowProcessor(BlockingQueue<RowBatch>[] queues, int shards, int batchSize) {
        this.queues = queues;
        this.shards = shards;
        this.batchSize = batchSize;
        this.currentBatchPerShard = new RowBatch[shards];
        for (int i = 0; i < shards; i++) currentBatchPerShard[i] = new RowBatch(batchSize);
    }

    @Override
    public void processStarted(ParsingContext context) {
        // no-op
    }

    @Override
    public void rowProcessed(String[] row, ParsingContext context) {
        // row length expected 6
        String id = row[IDX_CAMPAIGN];
        long imp = Long.parseLong(row[IDX_IMP]);
        long clk = Long.parseLong(row[IDX_CLICKS]);
        double sp = Double.parseDouble(row[IDX_SPEND]);
        long conv = Long.parseLong(row[IDX_CONV]);

        int shard = (id.hashCode() & 0x7fffffff) % shards;
        RowBatch b = currentBatchPerShard[shard];
        b.add(id, imp, clk, sp, conv);

        if (b.isFull()) flushShard(shard);

        rowsSeen.incrementAndGet();
    }

    @Override
    public void processEnded(ParsingContext context) {
        // flush remaining partial batches
        for (int shard = 0; shard < shards; shard++) {
            if (!currentBatchPerShard[shard].isEmpty()) flushShard(shard);
        }
        // You can print count if you want:
        // System.out.println("Rows processed: " + rowsSeen.get());
    }

    private void flushShard(int shard) {
        RowBatch full = currentBatchPerShard[shard];
        try {
            queues[shard].put(full);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        currentBatchPerShard[shard] = new RowBatch(batchSize);
    }
}