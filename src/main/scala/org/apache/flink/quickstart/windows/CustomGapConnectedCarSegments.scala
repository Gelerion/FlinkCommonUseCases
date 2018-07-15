package org.apache.flink.quickstart.windows

import java.lang
import java.time.Instant

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{ConnectedCarEvent, StoppedSegment}
import org.apache.flink.api.scala._
import org.apache.flink.quickstart.other.OutOfOrderTimestampAssigner
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * Created by denis.shuvalov on 15/07/2018.
  */
object CustomGapConnectedCarSegments {
  private val verbose: Boolean = false

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val carSource = "D:/Java/Learning/flinkscalaproject/src/main/resources/enviroCar/carOutOfOrder.csv"

    val carData: DataStream[ConnectedCarEvent] = env.readTextFile(carSource)
      .map(event => ConnectedCarEvent.fromString(event))
      .assignTimestampsAndWatermarks(new OutOfOrderTimestampAssigner)

    carData
      .keyBy(_.carId)
      .window(GlobalWindows.create())
      .trigger(new CarStoppedTrigger)
      .evictor(new DrivingEvictor)
      .apply(new CreateStoppedSegment())
      .print()

    env.execute("CustomGapConnectedCarSegments")
  }

  class CreateStoppedSegment extends WindowFunction[ConnectedCarEvent, StoppedSegment, String, GlobalWindow] {
    def apply(key: String,
              window: GlobalWindow,
              input: Iterable[ConnectedCarEvent],
              out: Collector[StoppedSegment]): Unit = {
      if (verbose) println(s" [Window] Got ${input.size} events")
      val segment = new StoppedSegment(input.asJava)
      if (segment.length > 0) { //it is possible to receive two or more stopped events in a row
        if (verbose) println(s" [Window] Consuming  ${segment.length} events")
        out.collect(segment)
      }
    }
  }

  class CarStoppedTrigger extends Trigger[ConnectedCarEvent, GlobalWindow] {
    def onElement(event: ConnectedCarEvent,
                  timestamp: Long,
                  window: GlobalWindow,
                  ctx: Trigger.TriggerContext): TriggerResult = {

      // if this is a stop event, set a timer
      if (event.speed == 0.0) {
        if (verbose) println(s" [Trigger] Stop event received with timestamp ${Instant.ofEpochMilli(event.timestamp)}")
        ctx.registerEventTimeTimer(event.timestamp)
      }

      TriggerResult.CONTINUE
    }

    def onEventTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      if (verbose) println(s" [Trigger - onEventTime] Firing trigger ${Instant.ofEpochMilli(time)}")
      TriggerResult.FIRE
    }

    def onProcessingTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    def clear(window: GlobalWindow, ctx: Trigger.TriggerContext) = {}
  }

  class DrivingEvictor extends Evictor[ConnectedCarEvent, GlobalWindow] {

    def evictBefore(events: lang.Iterable[TimestampedValue[ConnectedCarEvent]],
                    size: Int, window: GlobalWindow,
                    ctx: Evictor.EvictorContext): Unit = {

    }

    def evictAfter(events: lang.Iterable[TimestampedValue[ConnectedCarEvent]],
                   size: Int,
                   window: GlobalWindow,
                   ctx: Evictor.EvictorContext): Unit = {
      if (verbose) println(s" [evictAfter] Events $size received")

      val earliestStopTime = ConnectedCarEvent.earliestStopElement(events)

      if (verbose) println(s" [evictAfter] Earliest stop time ${Instant.ofEpochMilli(earliestStopTime)}")
      val it = events.iterator()
      while (it.hasNext) {
        val timestampedEvent = it.next()
        if (earliestStopTime >= timestampedEvent.getTimestamp) it.remove()
      }

      if (verbose) println(s" [evictAfter] Evicted ${size - events.asScala.size} events")
    }
  }

}
