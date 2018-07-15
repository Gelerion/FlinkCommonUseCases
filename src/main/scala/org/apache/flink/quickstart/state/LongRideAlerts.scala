package org.apache.flink.quickstart.state

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * Created by denis.shuvalov on 08/07/2018.
  *
  * The goal of the “Long Ride Alerts” exercise is to indicate whenever a taxi ride started two hours ago, and is still ongoing.
  *
  * Input Data
  * The input data of this exercise is a DataStream of taxi ride events.
  *
  * Expected Output
  * The result of the exercise should be a DataStream[TaxiRide] that only contains START events of taxi rides which have
  * no matching END event within the first two hours of the ride.
  *
  * The resulting stream should be printed to standard out.
  *
  * Here are the rideIds and start times of the first few rides that go on for more than two hours, but you might
  * want to print other info as well:
  *
  * > 2758,2013-01-01 00:10:13
  * > 7575,2013-01-01 00:20:23
  * > 22131,2013-01-01 00:47:03
  */
object LongRideAlerts {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val taxiRidePath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiRides.gz"

    val rides: KeyedStream[TaxiRide, Long] = env
      .addSource(new TaxiRideSource(taxiRidePath, 1800))
      .keyBy(_.rideId)

    rides.process(new LongRidesDetector)
      .print()

    env.execute("Long Taxi Rides")

  }

  class LongRidesDetector extends KeyedProcessFunction[Long, TaxiRide, LongRideReport] {
    val twoHours: Long = 120 * 60 * 1000

    // keyed, managed state
    // holds an END event if the ride has ended, otherwise a START event
    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("ride", classOf[TaxiRide])
    )

    def processElement(ride: TaxiRide,
                       ctx: KeyedProcessFunction[Long, TaxiRide, LongRideReport]#Context,
                       out: Collector[LongRideReport]): Unit = {
      if (ride.isStart) {
        // the matching END might have arrived first; don't overwrite it
        if (rideState.value() == null) {
          rideState.update(ride)
        }
      }
      else {
        rideState.update(ride)
      }

      ctx.timerService().registerEventTimeTimer(ride.getEventTime + twoHours)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, TaxiRide, LongRideReport]#OnTimerContext,
                         out: Collector[LongRideReport]): Unit = {
      val savedRide = rideState.value

      if (savedRide != null && savedRide.isStart) {
        out.collect(LongRideReport(savedRide.rideId, savedRide.getEventTime, timestamp))
      }

      rideState.clear()
    }
  }

  case class LongRideReport(rideId: Long, startTime: Long, timestamp: Long)
}
