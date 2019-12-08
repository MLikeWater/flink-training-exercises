package com.ververica.flinktraining.solutions.datastream_java.basics;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Description: TODO
 * @Author: shouzhuangjiang
 * @Create: 2019-12-08 15:54
 */
public class ConnectedStreamFlatMapSolution {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // control 会被 flatMap1 处理
        // 从 streamOfWords 流中过滤出不在 control 流中的单词
        DataStream<String> control = env.fromElements("DROP", "IGNORE").keyBy(x -> x);
        // streamOfWords 会被 flatMap2 处理
        // 因为 data 和 artisans 单词不在 control 流中，所以其状态在 flatMap1 中为 null，不为 TRUE，因此 streamOfWords 在调用 flatMap2 时满足 blocked.value() == null， 则会被输出
        DataStream<String> streamOfWords = env.fromElements("data", "DROP", "artisans", "IGNORE").keyBy(x -> x);

        // control 流连接 streamOfWords 流，两个流都是以单词做 keyBy，即 key 值为单词
        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
        // key 状态使用 Boolean 值保存，blocked 用于判断每个单词是否在 control 流中
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration config) {
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        // control.connect(streamOfWords) 表明 control 流中的元素会被 flatMap1 处理，streamOfWords 流中的元素会被 flatMap2 处理
        @Override
        public void flatMap1(String control_value, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        // 对于不在 control 流中的元素，其状态不为 TRUE，即 blocked.value() == null，从而被 flatMap2 处理时，会被 out 输出
        @Override
        public void flatMap2(String data_value, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(data_value);
            }
        }
    }
}