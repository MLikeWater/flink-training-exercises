package com.ververica.flinktraining.solutions.datastream_scala.basics

/** Case class to hold the SensorReading data. */
case class SensorReading(id: String, timestamp: Long, temperature: Double)