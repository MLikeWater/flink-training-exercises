package com.ververica.flinktraining.exercises.datastream_java.datatypes;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Description: TODO
 * @Author: shouzhuangjiang
 * @Create: 2019-12-05 15:05
 */
public class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {
    @Override
    public EnrichedRide map(TaxiRide ride) throws Exception {
        return new EnrichedRide(ride);
    }
}
