package org.example.model;

public class Agg {
    private long impressions, clicks, conversions;
    private double spend;

    public void add(long imp, long clk, double sp, long conv) {
        impressions += imp;
        clicks += clk;
        spend += sp;
        conversions += conv;
    }

    public long getImpressions() {
        return impressions;
    }

    public long getClicks() {
        return clicks;
    }

    public long getConversions() {
        return conversions;
    }

    public double getSpend() {
        return spend;
    }
}