package com.ververica.flinktraining.exercises.datastream_java.datatypes;

import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;

/**
 * @Description: Ride events add columns
 * @Author: shouzhuangjiang
 * @Create: 2019-12-05 14:56
 */
public class EnrichedRide extends TaxiRide {

    public int startCell;
    public int endCell;

    public EnrichedRide() {

    }

    public EnrichedRide(TaxiRide ride) {
        this.rideId = ride.rideId;
        this.isStart = ride.isStart;
        this.startTime = ride.startTime;
        this.endTime = ride.endTime;
        this.startLon = ride.startLon;
        this.startLat = ride.startLat;
        this.endLon = ride.endLon;
        this.endLat = ride.endLat;
        this.passengerCnt = ride.passengerCnt;
        this.taxiId = ride.taxiId;
        this.driverId = ride.driverId;

        this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
        this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
    }

    public String toString() {
        return super.toString() + "," +
                Integer.toString(this.startCell) + "," +
                Integer.toString(this.endCell);
    }
}



