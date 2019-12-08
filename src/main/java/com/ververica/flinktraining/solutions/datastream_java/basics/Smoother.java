package com.ververica.flinktraining.solutions.datastream_java.basics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * @Description: TODO
 * @Author: shouzhuangjiang
 * @Create: 2019-12-07 17:25
 */
public class Smoother extends RichMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {
    private ValueState<MovingAverage> averageState;

    @Override
    public void open (Configuration conf) {
        ValueStateDescriptor<MovingAverage> descriptor =
                new ValueStateDescriptor<>("moving average", MovingAverage.class);
        averageState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public Tuple2<String, Double> map (Tuple2<String, Double> item) throws Exception {
        // access the state for this key
        MovingAverage average = averageState.value();

        // create a new MovingAverage (with window size 2) if none exists for this key
        if (average == null) {
            average = new MovingAverage(2);
        }

        // add this event to the moving average
        average.add(item.f1);
        averageState.update(average);

        // return the smoothed result
        return new Tuple2(item.f0, average.getAverage());
    }
}