package org.apache.flink.quickstart.state

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{TaxiFare, TaxiRide}
import com.dataartisans.flinktraining.exercises.datastream_java.sources.{CheckpointedTaxiFareSource, CheckpointedTaxiRideSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * Created by denis.shuvalov on 02/07/2018.
  *
  * The problem with using JoinRidesWithFares (RichCoFlatMap) implementation
  * for this application is that in a real-world system we have to expect that some records will be lost or corrupted.
  * This means that over time we will accumulate an ever-growing collection of unmatched TaxiRide and TaxiFare records
  * waiting to be matched with event data that will never arrive. Eventually our enrichment job will run out of memory.
  *
  * You can solve this by using the timers available in a CoProcessFunction to eventually expire and clear any
  * unmatched state that is being kept.
  */
object ExpiringStateJoin {
  val unmatchedRides = new OutputTag[TaxiRide]("unmatchedRides") {}
  val unmatchedFares = new OutputTag[TaxiFare]("unmatchedFares") {}

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val taxiRidePath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiRides.gz"
    val taxiFaresPath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiFares.gz"

    val servingSpeedFactor = 600

    //Simulating Missing Data
    //You should arrange for some predictable fraction of the input records to be missing, so you can verify that you
    //are correctly handling clearing the corresponding state. The exercise code does this in a FilterFunction on the
    //TaxiRides. It drops all END events, and every 1000th START event.
    val rides = env.addSource(new CheckpointedTaxiRideSource(taxiRidePath, servingSpeedFactor))
      .filter { ride => ride.isStart && (ride.rideId % 1000 != 0) }

    val fares = env.addSource(new CheckpointedTaxiFareSource(taxiFaresPath, servingSpeedFactor))

    /*
    Expected Output
    The result of this exercise is a data stream of Tuple2<TaxiRide, TaxiFare> records, one for each distinct rideId.
    You should ignore the END events, and only join the event for the START of each ride with its corresponding fare data.

    In order to clearly see what is happening, create side outputs where you collect each unmatched TaxiRide and TaxiFare
    that is discarded in the OnTimer method of the CoProcessFunction.

    Once the join is basically working, don’t bother printing the joined records. Instead, print to standard out everything
    going to the side outputs, and verify that the results make sense. If you use the filter proposed above, then you should
    see something like this. These are TaxiFare records that were stored in state for a time, but eventually discarded because
    the matching TaxiRide events hadn’t arrived.

    1> 1000,2013000992,2013000989,2013-01-01 00:05:38,CSH,0.0,4.8,18.3
    3> 2000,2013001967,2013001964,2013-01-01 00:08:25,CSH,0.0,0.0,17.5
     */

    val keyedRides = rides.keyBy(_.rideId)
    val keyedFares = fares.keyBy(_.rideId)

    val processed = keyedRides.connect(keyedFares)
      .process(new ConnectWithExpiringState)

    processed.getSideOutput(unmatchedFares).print()
    processed.getSideOutput(unmatchedRides).print()

    env.execute("Evict outdated state")
  }

  class ConnectWithExpiringState extends CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {

//    lazy val outputTag: OutputTag[(TaxiRide, TaxiFare)] = OutputTag[(TaxiRide, TaxiFare)]("side-output")

    lazy val taxiRideValue: ValueState[TaxiRide] = getRuntimeContext
      .getState(new ValueStateDescriptor[TaxiRide]("taxiRide", classOf[TaxiRide]))

    lazy val taxiFaresValue: ValueState[TaxiFare] = getRuntimeContext
      .getState(new ValueStateDescriptor[TaxiFare]("taxiFare", classOf[TaxiFare]))

    def processElement1(taxiRide: TaxiRide,
                        ctx: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                        out: Collector[(TaxiRide, TaxiFare)]): Unit = {

      val taxiFare = taxiFaresValue.value()

      if (taxiFare == null) {
        taxiRideValue.update(taxiRide)
      }
      else {
        taxiFaresValue.clear()
        out.collect(taxiRide, taxiFare)
      }

      // Only one timer per timestamp will be registered.
      ctx.timerService().registerEventTimeTimer(taxiRide.getEventTime + (600 * 1000))
    }

    def processElement2(taxiFare: TaxiFare,
                        ctx: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                        out: Collector[(TaxiRide, TaxiFare)]): Unit = {

      val taxiRide = taxiRideValue.value()

      if (taxiRide == null) {
        taxiFaresValue.update(taxiFare)
      }
      else {
        taxiRideValue.clear()
        out.collect(taxiRide, taxiFare)
      }

      ctx.timerService().registerEventTimeTimer(taxiFare.getEventTime + (600 * 1000))
    }

    override def onTimer(timestamp: Long,
                ctx: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#OnTimerContext,
                out: Collector[(TaxiRide, TaxiFare)]): Unit = {

      val taxiRide = taxiRideValue.value()
      if (taxiRide != null) {
        ctx.output(unmatchedRides, taxiRide)
        taxiRideValue.clear()
      }

      val taxiFare = taxiFaresValue.value()
      if (taxiFare != null) {
        ctx.output(unmatchedFares, taxiFare)
        taxiFaresValue.clear()
      }

    }
  }
}
