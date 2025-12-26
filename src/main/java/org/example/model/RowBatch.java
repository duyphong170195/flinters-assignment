package org.example.model;

public class RowBatch {
    private static final RowBatch POISON = new RowBatch(0, true);

    private final String[] campaignId;
    private final long[] impressions, clicks, conversions;
    private final double[] spend;
    private int size = 0;
    private final boolean poison;

    public RowBatch(int capacity) {
        this(capacity, false);
    }

    private RowBatch(int capacity, boolean poison) {
        this.poison = poison;
        this.campaignId = poison ? null : new String[capacity];
        this.impressions = poison ? null : new long[capacity];
        this.clicks = poison ? null : new long[capacity];
        this.conversions = poison ? null : new long[capacity];
        this.spend = poison ? null : new double[capacity];
    }

    public void add(String id, long imp, long clk, double sp, long conv) {
        int i = size++;
        campaignId[i] = id;
        impressions[i] = imp;
        clicks[i] = clk;
        spend[i] = sp;
        conversions[i] = conv;
    }

    boolean isFull() {
        return size == campaignId.length;
    }

    boolean isEmpty() {
        return size == 0;
    }

    public static RowBatch getPOISON() {
        return POISON;
    }

    public String[] getCampaignId() {
        return campaignId;
    }

    public long[] getImpressions() {
        return impressions;
    }

    public long[] getClicks() {
        return clicks;
    }

    public long[] getConversions() {
        return conversions;
    }

    public double[] getSpend() {
        return spend;
    }

    public int getSize() {
        return size;
    }

    public boolean isPoison() {
        return poison;
    }
}