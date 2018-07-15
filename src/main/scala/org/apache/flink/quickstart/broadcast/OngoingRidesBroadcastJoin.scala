package org.apache.flink.quickstart.broadcast

import java.util.Locale

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.KeyedStateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector
import org.joda.time.format.DateTimeFormat


/**
  * Created by denis.shuvalov on 02/07/2018.
  */
object OngoingRidesBroadcastJoin {

  def main(args: Array[String]): Unit = {

    val taxiRidePath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiRides.gz"

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dummyBroadcastState = new MapStateDescriptor[Long, Long]("dummy", Types.of[Long], Types.of[Long])

    val queryStream: BroadcastStream[String] = env
      .socketTextStream("localhost", 9999)
      .assignTimestampsAndWatermarks(new QueryStreamAssigner)
      .broadcast(dummyBroadcastState)

    val rides: KeyedStream[TaxiRide, Long] = env
      .addSource(new TaxiRideSource(taxiRidePath, 60, 600))
      .keyBy(_.taxiId)

    rides.connect(queryStream)
      .process(new QueryFunction)

    env.execute("OngoingRidesBroadcastJoin")

  }

  class QueryFunction extends KeyedBroadcastProcessFunction[Long, TaxiRide, String, TaxiRide] {
    lazy val latestRideState: ValueState[TaxiRide] = getRuntimeContext.getState(taxiDescriptor)
    lazy val taxiDescriptor = new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide])

    def processElement(ride: TaxiRide,
                       readOnlyContext: KeyedBroadcastProcessFunction[Long, TaxiRide, String, TaxiRide]#ReadOnlyContext,
                       out: Collector[TaxiRide]): Unit = {
      // For every taxi, let's store the most up-to-date information.
      // TaxiRide implements Comparable to make this easy.
      val latestRide = latestRideState.value()
      if (ride.compareTo(latestRide) == 1) latestRideState.update(ride)
    }

    def processBroadcastElement(msg: String,
                                ctx: KeyedBroadcastProcessFunction[Long, TaxiRide, String, TaxiRide]#Context,
                                out: Collector[TaxiRide]): Unit = {
      val timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        .withLocale(Locale.US)
        .withZoneUTC

      val thresholdInMinutes = msg.toLong
      val wm = ctx.currentWatermark

      print("QUERY: " + thresholdInMinutes + " minutes at " + timeFormatter.print(wm))

      // Collect to the output all ongoing rides that started at least thresholdInMinutes ago.
      ctx.applyToKeyedState(taxiDescriptor, new KeyedStateFunction[Long, ValueState[TaxiRide]] {
        def process(taxiId: Long, taxiState: ValueState[TaxiRide]): Unit = {
          val ride = taxiState.value()
          if (ride.isStart) {
            val minutes = (wm - ride.getEventTime) / 60000
            if (ride.isStart && (minutes >= thresholdInMinutes)) out.collect(ride)
          }
        }
      })
    }
  }

  // Once the two streams are connected, the Watermark of the KeyedBroadcastProcessFunction
  // operator will be the minimum of the Watermarks of the two connected streams. Our query stream
  // has a default Watermark at Long.MIN_VALUE, and this will hold back the event time clock of
  // the connected stream, unless we do something about it.
  class QueryStreamAssigner extends AssignerWithPeriodicWatermarks[String] {

    def getCurrentWatermark: Watermark = {
      Watermark.MAX_WATERMARK
    }

    def extractTimestamp(element: String, previousElementTimestamp: Long): Long = 0
  }
}
