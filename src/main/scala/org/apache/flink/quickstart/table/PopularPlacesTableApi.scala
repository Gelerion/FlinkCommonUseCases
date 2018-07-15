package org.apache.flink.quickstart.table

import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils.{IsInNYC, ToCellId, ToCoords}
import com.dataartisans.flinktraining.exercises.table_java.sources.TaxiRideTableSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.types.Row

/**
  * Created by denis.shuvalov on 04/07/2018.
  *
  * The task of the “Popular Places” exercise is to identify popular places from a table of taxi rides records just like
  * the previous Popular Places exercise. This is done by counting every five minutes the number of taxi rides that
  * started and ended in the same area within the last 15 minutes. Arrival and departure locations should be separately
  * counted. Only locations with more arrivals or departures than a provided popularity threshold should be forwarded
  * to the result.
  *
  * You can implement a solution for the exercise with Flink’s Table API or SQL interface.
  *
  * Expected Output
  * The result of this exercise is a Table with the following schema:
  *
  * coords         : (Float, Float) // pair of longitude/latitude
  * isStart        : Boolean        // flag indicating departure or arrival count
  * wstart         : Timestamp      // the start time of the sliding window
  * wend           : Timestamp      // the end time of the sliding window
  * popCnt         : Long           // the number of rides departing or arriving
  */
object PopularPlacesTableApi {

  def main(args: Array[String]): Unit = {
    val taxiRidePath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiRides.gz"

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // register TaxiRideTableSource as table "TaxiRides" in the TableEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerTableSource("TaxiRides",
      new TaxiRideTableSource(taxiRidePath, 60, 60))

//    tEnv.registerFunction("isInNyc", new GeoUtils.IsInNYC)
//    tEnv.registerFunction("toCellId", new GeoUtils.ToCellId)
//    tEnv.registerFunction("toCoords", new GeoUtils.ToCoords)

    // ------------------- Table API
    // create user-defined functions
    val isInNYC = new IsInNYC
    val toCellId = new ToCellId
    val toCoords = new ToCoords

    // use UDF in SQL
    //tEnv.sqlQuery("SELECT isInNyc(startLon, startLat) FROM TaxiRides")

    // scan the TaxiRides table
    val rides: Table = tEnv.scan("TaxiRides")
    //use UDF in Table API
    //val nycRides: Table = rides.select("isInNyc(startLon, startLat)")

    val popPlaces = rides
      .filter(isInNYC('startLon, 'startLat) && isInNYC('endLon, 'endLat))
      .select('eventTime, 'isStart, 'isStart.?(toCellId('startLon, 'startLat), toCellId('endLon, 'endLat)) as 'cell)
      .window(Slide over 15.minutes every 5.minutes on 'eventTime as 'w)
      .groupBy('cell, 'isStart, 'w)
      .select(toCoords('cell) as 'location, 'isStart, 'w.start as 'start, 'w.end as 'end, 'isStart.count as 'popCnt)
      .filter('popCnt > 20)

    tEnv.toAppendStream[Row](popPlaces).print()
    env.execute("Calculate popular places Table API")
  }
}
