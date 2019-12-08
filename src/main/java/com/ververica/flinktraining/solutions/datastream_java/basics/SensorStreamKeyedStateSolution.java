package com.ververica.flinktraining.solutions.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description: TODO
 * @Author: shouzhuangjiang
 * @Create: 2019-12-07 17:17
 */
public class SensorStreamKeyedStateSolution {
    public static void main(String[] args) throws Exception {
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        // receive each sensor from a socket
        // nc -l 1000
        DataStream<String> text = env.socketTextStream("localhost", 8000, "\n", 10);

        // processing each sensor data
        DataStream<Tuple2<String, Double>> input = text.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String sensordata) throws Exception {

                // each sensor format:
                // S000001,8.0
                // S000002,3.0
                // S000001,6.0
                // S000001,3.0
                // S000002,5.0
                // S000002,7.0
                String[] record = sensordata.split(",");
                if (record.length == 2) {
                    return new Tuple2<>(record[0], Double.valueOf(record[1]));
                } else {
                    System.err.println("异常数据: " + sensordata);
                    // 返回异常数据
                    return new Tuple2<>("E000000", 0.0);
                }
            }
        });


        DataStream<Tuple2<String, Double>> smoothed = input.keyBy(0).map(new Smoother());

        // print result
        // 4> (S000001,0.0)
        // 4> (S000002,0.0)
        // 4> (S000001,0.0)
        // 4> (S000001,4.5)
        // 4> (S000002,0.0)
        // 4> (S000002,6.0)
        smoothed.print();

        env.execute("SensorStreamKeyedStateSolution Job");
    }

}
