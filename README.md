Implementation Overview
Key goals

Streaming processing (do not load the full CSV into memory)

Fast parsing using univocity-parsers

Concurrent aggregation using multiple worker threads

Memory efficient by storing only per-campaign aggregates

Concurrency design

Single producer thread parses CSV (streaming).

Parsed rows are dispatched into sharded bounded queues based on hash(campaign_id) % threads.

Each worker owns a local HashMap<String, Agg> (no locks).

Rows are grouped into batches to reduce queue overhead.

At the end, top-10 lists are computed without sorting the full dataset (heap of size 10).

FLOW:
CSV stream
↓
Producer parses rows
↓ shard by campaign_id
BlockingQueue[0] → Worker0 → HashMap0  
BlockingQueue[1] → Worker1 → HashMap1
...
BlockingQueue[N-1] → WorkerN-1 → HashMapN-1
↓
Sau khi xong: scan tất cả HashMap shard để lấy top10

Metrics

CTR = total_clicks / total_impressions (if impressions = 0 => CTR = 0)

CPA = total_spend / total_conversions (if conversions = 0 => CPA is empty and excluded from top10_cpa)

Libraries Used

univocity-parsers: high-performance streaming CSV parser

(Optionally, you can mention Maven Shade plugin for fat-jar packaging.)

Setup
1) Download dataset

From the repository folder:

Download ad_data.csv.zip

Unzip:

unzip ad_data.csv.zip

then paste file csv to folder flinters-assignment

2) Build
   mvn -q clean package


This produces:

target/aggregator.jar (fat jar)

Run (CLI)
java -jar target/aggregator.jar \
--input ad_data.csv \
--output results/ \
--threads 8 \
--batchSize 4096 \
--queueCapacity 50
java -jar target/aggregator.jar --input ad_data.csv --output results/ --threads 8 --batchSize 4096 --queueCapacity 50

Outputs:

results/top10_ctr.csv

results/top10_cpa.csv

Recommended parameters

--threads: number of worker threads (usually CPU cores or slightly less)

--batchSize: 4096 or 8192

--queueCapacity: 20–50 (bounded; keeps memory stable)