package org.apache.flink.quickstart.other

import java.time.Instant

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Created by denis.shuvalov on 12/07/2018.
  *
  * You will need to figure out how to generate appropriate watermarks. You should test each exercise with
  * both the in-order and out-of-order data files, and make sure they produce consistent results.
  */
object EnviroCar {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //env.getConfig.setAutoWatermarkInterval(1)

//    val carSource = "D:/Java/Learning/flinkscalaproject/src/main/resources/enviroCar/carInOrder.csv"
    val carSource = "D:/Java/Learning/flinkscalaproject/src/main/resources/enviroCar/carOutOfOrder.csv"

    val carData: DataStream[ConnectedCarEvent] = env.readTextFile(carSource)
      .map(event => ConnectedCarEvent.fromString(event))
      .assignTimestampsAndWatermarks(new OutOfOrderTimestampAssigner)

    carData.keyBy(_.carId)
      .process(new CarEventsSorter)
      .print()

    env.execute("Connected car exercise")
  }

}

class CarEventsSorter extends KeyedProcessFunction[String, ConnectedCarEvent, ConnectedCarEvent] {
  lazy val sortedCarEvents: ValueState[mutable.PriorityQueue[ConnectedCarEvent]] = getRuntimeContext.getState(
    new ValueStateDescriptor[mutable.PriorityQueue[ConnectedCarEvent]]("sorted events", Types.of[mutable.PriorityQueue[ConnectedCarEvent]])
  )

  def processElement(carEvent: ConnectedCarEvent,
                     ctx: KeyedProcessFunction[String, ConnectedCarEvent, ConnectedCarEvent]#Context,
                     out: Collector[ConnectedCarEvent]): Unit = {
    val timerService = ctx.timerService()
    //println(s"Watermark: ${Instant.ofEpochMilli(timerService.currentWatermark())}, event timestamp ${Instant.ofEpochMilli(ctx.timestamp())}")

    if (ctx.timestamp() > timerService.currentWatermark()) {
      //println(s"Register timer with even time ${Instant.ofEpochMilli(ctx.timestamp())}")
      //println(s"Save car event ${Instant.ofEpochMilli(ctx.timestamp())}")
      var queue = sortedCarEvents.value()
      if (queue == null) queue = mutable.PriorityQueue.empty[ConnectedCarEvent].reverse
      queue.enqueue(carEvent)
      sortedCarEvents.update(queue)
      timerService.registerEventTimeTimer(ctx.timestamp())
    }
  }

  //Flink guarantees to never call the timer callback and the process element function concurrently
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, ConnectedCarEvent, ConnectedCarEvent]#OnTimerContext,
                       out: Collector[ConnectedCarEvent]): Unit = {
    val wm = ctx.timerService().currentWatermark()

    //println(s"Timer '${ctx.getCurrentKey}' is fired for '${Instant.ofEpochMilli(timestamp)}', current watermark is '${Instant.ofEpochMilli(wm)}'")

    val carEvents = sortedCarEvents.value()
    //println(s"Is car events empty ${carEvents.isEmpty}, size ${carEvents.size}")

    if (carEvents.nonEmpty) {
      var carEvent = carEvents.head
      while (carEvent != null && wm >= carEvent.timestamp) {
        carEvents.dequeue()
        println(s"Collecting car event with timestamp ${Instant.ofEpochMilli(carEvent.timestamp)}")
        //out.collect(carEvent)
        if (carEvents.nonEmpty) carEvent = carEvents.head
        else carEvent = null
      }
    }
  }
}

//this one doesn;t start with first event, first events are fired with Long.MIN_VALUE watermark
//see https://stackoverflow.com/questions/48576391/flink-how-set-up-initial-watermark
//class OutOfOrderTimestampAssigner extends BoundedOutOfOrdernessTimestampExtractor[ConnectedCarEvent](Time.seconds(30)) {
//  def extractTimestamp(event: ConnectedCarEvent): Long = event.timestamp
//}
// as a solution we can use a punctuated assigner
class OutOfOrderTimestampAssigner extends AssignerWithPunctuatedWatermarks[ConnectedCarEvent] {

  def extractTimestamp(element: ConnectedCarEvent, previousElementTimestamp: Long): Long = {
    element.timestamp
  }

  def checkAndGetNextWatermark(lastElement: ConnectedCarEvent, extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp - 30 * 1000) //delay by 30 second
  }
}

/*
Insight wm with onTimer log

Watermark: -292275055-05-16T16:47:04.192Z, event timestamp 2017-01-20T05:36:35Z
Register timer with even time 2017-01-20T05:36:35Z

Watermark: 2017-01-20T05:36:05Z, event timestamp 2017-01-20T05:36:20Z
Register timer with even time 2017-01-20T05:36:20Z

Watermark: 2017-01-20T05:36:05Z, event timestamp 2017-01-20T05:36:25Z
Register timer with even time 2017-01-20T05:36:25Z

Watermark: 2017-01-20T05:36:05Z, event timestamp 2017-01-20T05:36:15Z
Register timer with even time 2017-01-20T05:36:15Z

Watermark: 2017-01-20T05:36:05Z, event timestamp 2017-01-20T05:36:30Z
Register timer with even time 2017-01-20T05:36:30Z

Watermark: 2017-01-20T05:36:05Z, event timestamp 2017-01-20T05:36:40Z
Register timer with even time 2017-01-20T05:36:40Z

Watermark: 2017-01-20T05:36:10Z, event timestamp 2017-01-20T05:36:45Z
Register timer with even time 2017-01-20T05:36:45Z

Watermark: 2017-01-20T05:36:15Z, event timestamp 2017-01-20T05:36:50Z
Timer '574e78cbe4b09078f97bbb4a' is fired ' 2017-01-20T05:36:15Z'
Register timer with even time 2017-01-20T05:36:50Z

Watermark: 2017-01-20T05:36:20Z, event timestamp 2017-01-20T05:36:55Z
Timer '574e78cbe4b09078f97bbb4a' is fired ' 2017-01-20T05:36:20Z'
Register timer with even time 2017-01-20T05:36:55Z

Watermark: 2017-01-20T05:36:25Z, event timestamp 2017-01-20T05:37:00Z
Timer '574e78cbe4b09078f97bbb4a' is fired ' 2017-01-20T05:36:25Z'
Register timer with even time 2017-01-20T05:37:00Z

Watermark: 2017-01-20T05:36:30Z, event timestamp 2017-01-20T05:37:05Z
Timer '574e78cbe4b09078f97bbb4a' is fired ' 2017-01-20T05:36:30Z'
Register timer with even time 2017-01-20T05:37:05Z

Watermark: 2017-01-20T05:36:35Z, event timestamp 2017-01-20T05:37:10Z
Timer '574e78cbe4b09078f97bbb4a' is fired ' 2017-01-20T05:36:35Z'
Register timer with even time 2017-01-20T05:37:10Z

Watermark: 2017-01-20T05:36:40Z, event timestamp 2017-01-20T05:37:15Z
Timer '574e78cbe4b09078f97bbb4a' is fired ' 2017-01-20T05:36:40Z'
Register timer with even time 2017-01-20T05:37:15Z

Watermark: 2017-01-20T05:36:45Z, event timestamp 2017-01-20T05:37:20Z
Timer '574e78cbe4b09078f97bbb4a' is fired ' 2017-01-20T05:36:45Z'
Register timer with even time 2017-01-20T05:37:20Z

Watermark: 2017-01-20T05:36:50Z, event timestamp 2017-01-20T05:37:35Z
Timer '574e78cbe4b09078f97bbb4a' is fired ' 2017-01-20T05:36:50Z'
Register timer with even time 2017-01-20T05:37:35Z

Watermark: 2017-01-20T05:37:05Z, event timestamp 2017-01-20T05:37:25Z
Timer '574e78cbe4b09078f97bbb4a' is fired ' 2017-01-20T05:36:55Z'
Timer '574e78cbe4b09078f97bbb4a' is fired ' 2017-01-20T05:37:00Z'
Timer '574e78cbe4b09078f97bbb4a' is fired ' 2017-01-20T05:37:05Z'
 */
