package org.apache.flink.quickstart.fault.recovery

import java.util.concurrent.TimeUnit

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.{CheckpointedTaxiRideSource, TaxiRideSource}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
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
object LongRideAlertsWithFaultRecovery {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //Configure Flink to perform a consistent checkpoint of a program’s state every 1000ms.
    env.enableCheckpointing(1000)
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
      60,                    // 60 retries
      Time.of(10, TimeUnit.SECONDS)   // 10 secs delay
    ))

    //Note that by default, Flink’s checkpoints are persisted on the JobManager’s heap. This is usually fine for
    //development and testing, so long as your application doesn’t have large amounts of state. But this exercise
    //is likely to keep too much state for that to suffice, and you should configure Flink to use the filesystem
    //state backend instead:
    val backend: StateBackend = new FsStateBackend("file:///D:/Programms/Flink/state/tmp/checkpoints")
    env.setStateBackend(backend)

    val taxiRidePath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiRides.gz"

    val servingSpeed = 1800

    val rides: KeyedStream[TaxiRide, Long] = env
      //TaxiRideSource  does not checkpoint its state, so it is not suitable, we must use CheckpointedTaxiRideSource
      .addSource(new CheckpointedTaxiRideSource(taxiRidePath, servingSpeed))
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
