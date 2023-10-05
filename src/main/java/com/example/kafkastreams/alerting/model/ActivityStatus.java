package com.example.kafkastreams.alerting.model;

public class ActivityStatus {
    private String user;
    private Long totalRequestsCount = 0L;
    private Long deniedRequestsCount = 0L;
    private Double deniedRatio;

    public ActivityStatus() {
    }

    public ActivityStatus(String user, Long allowedRequestsCount, Long deniedRequestsCount, Double deniedRatio) {
        this.user = user;
        this.totalRequestsCount = allowedRequestsCount;
        this.deniedRequestsCount = deniedRequestsCount;
        this.deniedRatio = deniedRatio;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Long getTotalRequestsCount() {
        return totalRequestsCount;
    }

    public void incrementTotalRequestsCount() {
        this.totalRequestsCount++;
    }

    public Long getDeniedRequestsCount() {
        return deniedRequestsCount;
    }

    public void incrementDeniedRequestsCount() {
        this.deniedRequestsCount++;
    }

    public Double getDeniedRatio() {
        return deniedRatio;
    }

    public void setDeniedRatio(Double deniedRatio) {
        this.deniedRatio = deniedRatio;
    }
}
