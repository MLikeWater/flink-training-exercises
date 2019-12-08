package com.ververica.flinktraining.solutions.datastream_java.basics;

import java.util.LinkedList;

/**
 * @Description: TODO
 * @Author: shouzhuangjiang
 * @Create: 2019-12-07 17:20
 */
class MovingAverage {
    int size;
    double sum;
    double average;

    LinkedList<Double> list;

    public MovingAverage(int size) {
        this.list = new LinkedList<>();
        this.size = size;
    }

    public double add(double value) {
        sum += value;

        // Adds the specified element as the tail (last element) of this list.
        list.offer(value);
        if (list.size() <= size) {
            return sum / list.size();
        }

        sum -= list.poll();

        return average = sum / size;
    }

    public double getAverage() {
        return average;
    }
}