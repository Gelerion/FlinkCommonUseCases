package org.apache.flink.quickstart

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
  * Created by denis.shuvalov on 19/06/2018.
  */
object TaxiRideCleansing {

  def main(args: Array[String]): Unit = {

//    val params = ParameterTool.fromArgs(args)
//    val input = params.getRequired("input")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val taxiRidePath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiRides.gz"

    val rides: DataStream[TaxiRide] = env.addSource(new TaxiRideSource(taxiRidePath, 1800))

    val onlyInNYC: DataStream[TaxiRide] = rides.filter(taxiRide =>
      GeoUtils.isInNYC(taxiRide.startLat, taxiRide.startLon) &&
        GeoUtils.isInNYC(taxiRide.endLat, taxiRide.endLon))

    onlyInNYC.print()

    env.execute("Taxi Ride Cleansing")
  }


}
