package org.apache.flink.quickstart.state

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{TaxiFare, TaxiRide}
import com.dataartisans.flinktraining.exercises.datastream_java.sources.{TaxiFareSource, TaxiRideSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * Created by denis.shuvalov on 20/06/2018.
  */
object JoinRidesWithFares {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val taxiRidePath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiRides.gz"
    val taxiFaresPath = "D:/Java/Learning/flinkscalaproject/src/main/resources/taxi/nycTaxiFares.gz"

    val rides: DataStream[TaxiRide] = env
      .addSource(new TaxiRideSource(taxiRidePath, 1800))
      .filter(_.isStart)
      .keyBy(_.rideId)

    val fares: DataStream[TaxiFare] = env
      .addSource(new TaxiFareSource(taxiFaresPath, 1800))
      .keyBy(_.rideId)

    rides.connect(fares)
      .flatMap(new ConnectingRidesWithFires)
      .print()

    env.execute("Join rides with fares")

  }

  class ConnectingRidesWithFires extends RichCoFlatMapFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {
    var taxiRideState: ValueState[TaxiRide] = _
    var taxiFareState: ValueState[TaxiFare] = _

    override def open(parameters: Configuration): Unit = {
      val taxiRideStateDescriptor = new ValueStateDescriptor[TaxiRide]("taxiRide", classOf[TaxiRide])
      val taxiFareStateDescriptor = new ValueStateDescriptor[TaxiFare]("taxiFare", classOf[TaxiFare])
      taxiRideState = getRuntimeContext.getState(taxiRideStateDescriptor)
      taxiFareState = getRuntimeContext.getState(taxiFareStateDescriptor)
    }

    def flatMap1(ride: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val fare = taxiFareState.value()
      if (fare != null) {
        taxiFareState.clear()
        out.collect((ride, fare))
      }
      else {
        taxiRideState.update(ride)
      }
    }

    def flatMap2(fare: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val ride = taxiRideState.value()
      if (ride != null) {
        taxiRideState.clear()
        out.collect((ride, fare))
      }
      else {
        taxiFareState.update(fare)
      }
    }
  }

}
