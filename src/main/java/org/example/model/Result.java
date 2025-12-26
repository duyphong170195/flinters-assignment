package org.example.model;

public class Result {
    private final String campaignId;
    private final long totalImpressions, totalClicks, totalConversions;
    private final double totalSpend;
    private final double ctr;      // clicks / impressions
    private final Double cpa;      // spend / conversions (null if conversions == 0)

    public Result(String id, Agg a) {
        this.campaignId = id;
        this.totalImpressions = a.getImpressions();
        this.totalClicks = a.getClicks();
        this.totalSpend = a.getSpend();
        this.totalConversions = a.getConversions();
        this.ctr = (totalImpressions == 0) ? 0.0 : ((double) totalClicks) / totalImpressions;
        this.cpa = (totalConversions == 0) ? null : (totalSpend / totalConversions);
    }

    public String getCampaignId() {
        return campaignId;
    }

    public long getTotalImpressions() {
        return totalImpressions;
    }

    public long getTotalClicks() {
        return totalClicks;
    }

    public long getTotalConversions() {
        return totalConversions;
    }

    public double getTotalSpend() {
        return totalSpend;
    }

    public double getCtr() {
        return ctr;
    }

    public Double getCpa() {
        return cpa;
    }
}