package com.ververica.flinktraining.exercises.datastream_java.datatypes;

import com.ververica.flinktraining.solutions.datastream_java.basics.RideCleansingSolution;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Description: TODO
 * @Author: shouzhuangjiang
 * @Create: 2019-12-05 17:07
 */
public class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
    @Override
    public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
        FilterFunction<TaxiRide> valid = new RideCleansingSolution.NYCFilter();
        if (valid.filter(taxiRide)) {
            out.collect(new EnrichedRide(taxiRide));
        }
    }
}
