package org.apache.flink.quickstart.windows

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{ConnectedCarEvent, GapSegment}
import org.apache.flink.api.scala._
import org.apache.flink.quickstart.other.OutOfOrderTimestampAssigner
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._


/**
  * Created by denis.shuvalov on 15/07/2018.
  *
  * The objective of the Connected Car Sessions exercise is to divide the connected car data stream into session
  * windows, where a gap of more than 15 seconds should start a new session.
  */
object ConnectedCarSessions {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val carSource = "D:/Java/Learning/flinkscalaproject/src/main/resources/enviroCar/carOutOfOrder.csv"

    val carData: DataStream[ConnectedCarEvent] = env.readTextFile(carSource)
      .map(event => ConnectedCarEvent.fromString(event))
      .assignTimestampsAndWatermarks(new OutOfOrderTimestampAssigner)

    carData.keyBy(_.carId)
      .window(EventTimeSessionWindows.withGap(Time.seconds(15)))
      .apply(new CreateGapSegment())
      .print()

    env.execute("Connected Car Sessions")
  }

  class CreateGapSegment extends WindowFunction[ConnectedCarEvent, GapSegment, String, TimeWindow] {
    def apply(key: String,
              window: TimeWindow,
              input: Iterable[ConnectedCarEvent],
              out: Collector[GapSegment]): Unit = {
      out.collect(new GapSegment(input.asJava))
    }
  }
}
