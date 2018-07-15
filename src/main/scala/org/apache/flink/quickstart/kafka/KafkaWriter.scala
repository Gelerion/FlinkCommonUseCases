package org.apache.flink.quickstart.kafka

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.{GeoUtils, TaxiRideSchema}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
  * Created by denis.shuvalov on 11/07/2018.
  */
object KafkaWriter {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val taxiRidePath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiRides.gz"

    // events are out of order by max 60 seconds
    // events of 10 minutes are served in 1 second
    val rides: DataStream[TaxiRide] = env.addSource(new TaxiRideSource(taxiRidePath, 60,60))
    val nycRides: DataStream[TaxiRide] = rides.filter { r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat) }

    nycRides.addSink(new FlinkKafkaProducer011[TaxiRide](
      "localhost:9092",// broker list
      topic,                      // target topic
      new TaxiRideSchema)         // serialization schema
    )

    env.execute("Write NYC rides to kafka")
  }
}
