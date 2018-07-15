package org.apache.flink.quickstart.kafka

import java.util.Properties

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema
import org.apache.flink.api.scala._
import org.apache.flink.quickstart.windows.{GridCellRide, PopularPlacesReporter}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaConsumerBase}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * Created by denis.shuvalov on 11/07/2018.
  */
object PopularPlacesKafka {
  def main(args: Array[String]): Unit = {

    // set up streaming execution environment// set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // configure event-time characteristics
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // generate a Watermark every second
    env.getConfig.setAutoWatermarkInterval(1000)

    val properties = new Properties()
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "taxiRidesGroup")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // Always read topic from start

    //Note: A stream read from Kafka does not automatically have timestamps and watermarks assigned. You have to take
    //care of this yourself in order to make the event-time windows working. Otherwise the program won’t emit any results.
    val kafkaConsumer: FlinkKafkaConsumerBase[TaxiRide] = new FlinkKafkaConsumer011(topic, new TaxiRideSchema(), properties)
      //.setStartFromEarliest()
      .assignTimestampsAndWatermarks(new WaterMarkAssigner)

    val rides = env.addSource(kafkaConsumer)

    rides.map(GridCellRide(_))
      .keyBy { ride => (ride.gridCellId, ride.eventType) }
      .timeWindow(Time.minutes(15), Time.minutes(5))
      .apply(new PopularPlacesReporter)
      .print()

    env.execute("Popular places from kafka")

  }

  class WaterMarkAssigner extends BoundedOutOfOrdernessTimestampExtractor[TaxiRide](Time.seconds(60)) {
    override def extractTimestamp(ride: TaxiRide): Long = ride.getEventTime
  }

}
