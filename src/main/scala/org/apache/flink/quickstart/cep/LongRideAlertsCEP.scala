package org.apache.flink.quickstart.cep

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.quickstart.state.LongRideAlerts.LongRideReport
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

/**
  * Created by denis.shuvalov on 10/07/2018.
  */
object LongRideAlertsCEP {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val taxiRidePath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiRides.gz"

    val rides: KeyedStream[TaxiRide, Long] = env
      .addSource(new CheckpointedTaxiRideSource(taxiRidePath, 600))
      .filter { r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat) }
      .keyBy(_.rideId)

    val longRidePattern = Pattern
      .begin[TaxiRide]("startRide").where(ride => ride.isStart)
      .followedBy("endRide").where(ride => !ride.isStart) //notFollowedBy is not supported as a last part of a Pattern!
      .within(Time.hours(2))

    val longRidesStream: PatternStream[TaxiRide] = CEP.pattern(rides, longRidePattern)

    val outputTag = OutputTag[LongRideReport]("side-output")

    val select: DataStream[String] = longRidesStream.select(outputTag)
    { (pattern: collection.Map[String, Iterable[TaxiRide]], timestamp: Long) => { //timed out pattern (patternTimeoutFunction)
        val startRide = pattern("startRide").head
        LongRideReport(startRide.rideId, startRide.getEventTime, timestamp)
      }
    } { pattern: Map[String, Iterable[TaxiRide]] => //matched pattern  (patternSelectFunction)
      "ended ride"
    }

    val timeoutResult: DataStream[LongRideReport] = select.getSideOutput(outputTag)
    timeoutResult.print()

    env.execute("CEP Long rides alert")
  }

  def createAlert(pattern : collection.Map[String, Iterable[TaxiRide]]): LongRideReport = {
    val startRide = pattern("startRide").head
    LongRideReport(startRide.rideId, startRide.getEventTime, System.currentTimeMillis())
  }

}
