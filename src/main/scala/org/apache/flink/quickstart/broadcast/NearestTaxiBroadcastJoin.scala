package org.apache.flink.quickstart.broadcast

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Created by denis.shuvalov on 03/07/2018.
  *
  * Connect two streams:
  *   (1) a stream of TaxiRide events and
  *   (2) a query stream on which you can type queries.
  *
  * In this case a query is two comma-separated floating point numbers representing a (longitude, latitude) pair.
  * The expected output is a stream of rides ending after the query was received, with each successive ride being
  * closer to the requested location
  *
  * The intent of this exercise is to support many ongoing queries simultaneously. In a real application it would make
  * sense to think about how to eventually clear all of the state associated with obsolete queries.
  *
  * Parameters:
  * -input path-to-input-file
  *
  * Use nc -lk 9999 to establish a socket stream from stdin on port 9999
  *
  * Some good locations:
  *
  * -74, 41 					(Near, but outside the city to the NNW)
  * -73.7781, 40.6413 		(JFK Airport)
  * -73.977664, 40.761484	(Museum of Modern Art)
  */
object NearestTaxiBroadcastJoin {
  val queryDescriptor  = new MapStateDescriptor[Long, Query]("queries", Types.of[Long], Types.of[Query])
  val minQueryRetentionTime: Long = 15 * 60 * 1000 // 15 minutes
  val closestReportRetentionTime: Long = 8 * 60 * 60 * 1000 // 8 hours

  def main(args: Array[String]): Unit = {
    val taxiRidePath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiRides.gz"

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val queryStream: BroadcastStream[Query] = env
      .socketTextStream("localhost", 9999)
      .map(msg => {
        val parts = msg.split(",")
        Query(longitude = parts(0).toFloat, latitude = parts(1).toFloat)
      })
      .assignTimestampsAndWatermarks(new QueryStreamAssigner)
      .broadcast(queryDescriptor)

    val rides: KeyedStream[TaxiRide, Long] = env
      .addSource(new TaxiRideSource(taxiRidePath, 60, 600))
      .keyBy(_.taxiId)

    val reports: DataStream[(Long, Long, Float)] =
      rides.connect(queryStream)
        .process(new QueryFunction)

    reports
      .keyBy(_._1) //query id
      .process(new ClosestTaxi)
      .print()

    env.execute("Nearest Available Taxi")
  }

  case class Query(queryId: Long = Random.nextLong(), longitude: Float, latitude: Float, var timestamp: Long = 0)
  class QueryStreamAssigner extends AssignerWithPeriodicWatermarks[Query] {
    def getCurrentWatermark: Watermark = Watermark.MAX_WATERMARK
    def extractTimestamp(query: Query, previousElementTimestamp: Long): Long = 0
  }

  // Note that in order to have consistent results after a restore from a checkpoint, the
  // behavior of this method must be deterministic, and NOT depend on characteristics of an
  // individual sub-task.
  class QueryFunction extends KeyedBroadcastProcessFunction[Long, TaxiRide, Query, (Long, Long, Float)] {

    // Output (queryId, taxiId, euclidean distance) for every query, if the taxi ride is now ending.
    def processElement(ride: TaxiRide,
                       readOnlyCtx: KeyedBroadcastProcessFunction[Long, TaxiRide, Query, (Long, Long, Float)]#ReadOnlyContext,
                       out: Collector[(Long, Long, Float)]): Unit = {
      if(!ride.isStart) {
        val queries = readOnlyCtx.getBroadcastState(queryDescriptor).immutableEntries().asScala.map(_.getValue)
        val results = for (query <- queries) yield (query.queryId, ride.taxiId, euclidean(ride, query))
        results.foreach(out.collect)
      }
    }

    // distance in kilometers
    def euclidean(ride: TaxiRide, query: Query): Float =
      //sqrt(pow(ride.endLon - query.longitude, 2)  + pow(ride.endLat - query.latitude, 2)).toFloat
      GeoUtils.getEuclideanDistance(query.longitude, query.latitude, ride.endLon, ride.endLat).toFloat

    def processBroadcastElement(query: Query,
                                ctx: KeyedBroadcastProcessFunction[Long, TaxiRide, Query, (Long, Long, Float)]#Context,
                                collector: Collector[(Long, Long, Float)]): Unit = {

      val wm = ctx.currentWatermark()
      query.timestamp = wm
      println(s"processing query $query")

      val queries = ctx.getBroadcastState(queryDescriptor)
      queries.iterator().asScala.filter(e => (wm - e.getValue.timestamp) > minQueryRetentionTime).foreach(obsoleteQuery => {
        println(s"removing query ${obsoleteQuery.getValue}")
        queries.remove(obsoleteQuery.getKey)
      })

      ctx.getBroadcastState(queryDescriptor).put(query.queryId, query)
    }
  }

  // Only pass thru values that are new minima -- remove duplicates.
  class ClosestTaxi extends KeyedProcessFunction[Long, (Long, Long, Float), (Long, Long, Float)] {
    // store (taxiId, distance), keyed by queryId
    lazy val closest = getRuntimeContext.getState(new ValueStateDescriptor[(Long, Float)]("reports", classOf[(Long, Float)]))
    lazy val latestTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("latest timer", Types.of[Long]))

    // in and out tuples: (queryId, taxiId, distance)
    def processElement(report: (Long, Long, Float),
                       ctx: KeyedProcessFunction[Long, (Long, Long, Float), (Long, Long, Float)]#Context,
                       out: Collector[(Long, Long, Float)]): Unit = {

      if (closest.value() == null || report._3 < closest.value()._2) {
        val cleanUpTime = ctx.timestamp() + closestReportRetentionTime
        ctx.timerService().registerEventTimeTimer(cleanUpTime)
        latestTimer.update(cleanUpTime)

        closest.update(report._2, report._3)
        out.collect(report)
      }
    }

    override def onTimer(timestamp: Long,
                ctx: KeyedProcessFunction[Long, (Long, Long, Float), (Long, Long, Float)]#OnTimerContext,
                out: Collector[(Long, Long, Float)]): Unit = {
      // only clean up state if this timer is the latest timer for this key
      if (latestTimer.value() == timestamp) {
        println(s"cleaning ${ctx.getCurrentKey}")
        closest.clear()
        latestTimer.clear()
      }
    }
  }
}
