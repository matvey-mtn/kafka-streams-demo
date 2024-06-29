package com.example.kafkastreams.model;

public class MetricAccumulator {

    private int count;
    private int sum;
    private double average;

    public MetricAccumulator() {
        count = 0;
        sum = 0;
        average = 0;
    }

    public MetricAccumulator(int count, int sum, double average) {
        this.count = count;
        this.sum = sum;
        this.average = average;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }
}
