package org.example.model;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Args {
    private final Path input;
    private final Path outputDir;
    private final int threads;
    private final int batchSize;
    private final int queueCapacity;

    Args(Path input, Path outputDir, int threads, int batchSize, int queueCapacity) {
        this.input = input;
        this.outputDir = outputDir;
        this.threads = threads;
        this.batchSize = batchSize;
        this.queueCapacity = queueCapacity;
    }

    public static Args parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String k = args[i];
            if (k.startsWith("--")) {
                if (i + 1 >= args.length) usageAndExit("Missing value for " + k);
                m.put(k, args[++i]);
            } else {
                usageAndExit("Unknown argument: " + k);
            }
        }

        Path input = getPath(m, "--input", true);
        Path output = getPath(m, "--output", true);
        int threads = getInt(m, "--threads", Runtime.getRuntime().availableProcessors());
        int batchSize = getInt(m, "--batchSize", 4096);
        int queueCap = getInt(m, "--queueCapacity", 50);

        if (threads <= 0) usageAndExit("--threads must be > 0");
        if (batchSize <= 0) usageAndExit("--batchSize must be > 0");
        if (queueCap <= 0) usageAndExit("--queueCapacity must be > 0");

        return new Args(input, output, threads, batchSize, queueCap);
    }

    static Path getPath(Map<String, String> m, String key, boolean required) {
        String v = m.get(key);
        if (v == null) {
            if (required) usageAndExit("Missing " + key);
            return null;
        }
        return Paths.get(v);
    }

    static int getInt(Map<String, String> m, String key, int def) {
        String v = m.get(key);
        if (v == null) return def;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            usageAndExit("Invalid int for " + key + ": " + v);
            return def;
        }
    }

    static void usageAndExit(String msg) {
        System.err.println(msg);
        System.err.println("Usage:");
        System.err.println("  java -jar aggregator.jar --input ad_data.csv --output results/ " +
                "[--threads 8] [--batchSize 4096] [--queueCapacity 50]");
        System.exit(2);
    }

    public Path getInput() {
        return input;
    }

    public Path getOutputDir() {
        return outputDir;
    }

    public int getThreads() {
        return threads;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }
}