import sys.process._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import java.util.HashMap

val trip_update_topic = "trip_update_topic"
val vehicle_topic = "vehicle_topic"
val broker = "b-1.test.1tklkx.c2.kafka.us-east-1.amazonaws.com:9092,b-3.test.1tklkx.c2.kafka.us-east-1.amazonaws.com:9092,b-2.test.1tklkx.c2.kafka.us-east-1.amazonaws.com:9092"

object MTASubwayTripUpdates extends Serializable {

    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    @transient var producer : KafkaProducer[String, String] = null
    var msgId : Long = 1
    @transient var joined_query : StreamingQuery = null
    @transient var joined_query_s3 : StreamingQuery = null

    val spark = SparkSession.builder.appName("MSK streaming Example").getOrCreate()

    import spark.implicits._

    def start() = {
        //Start producer for kafka
        producer = new KafkaProducer[String, String](props)

        //Create a datastream from trip update topic
        val trip_update_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", broker).option("subscribe", trip_update_topic).option("startingOffsets", "latest").option("failOnDataLoss","false").load()

        //Create a datastream from vehicle topic
        val vehicle_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", broker).option("subscribe", vehicle_topic).option("startingOffsets", "latest").option("failOnDataLoss","false").load()

        // define schema of data

        val trip_update_schema = new StructType().add("trip", new StructType().add("tripId","string").add("startTime","string").add("startDate","string").add("routeId","string")).add("stopTimeUpdate",ArrayType(new StructType().add("arrival",new StructType().add("time","string")).add("stopId","string").add("departure",new StructType().add("time","string"))))

        val vehicle_schema = new StructType().add("trip", new StructType().add("tripId","string").add("startTime","string").add("startDate","string").add("routeId","string")).add("currentStopSequence","integer").add("currentStatus", "string").add("timestamp", "string").add("stopId","string")

        // covert datastream into a datasets and apply schema
        val trip_update_ds = trip_update_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
        val trip_update_ds_schema = trip_update_ds.select(from_json($"value", trip_update_schema).as("data")).select("data.*")
        trip_update_ds_schema.printSchema()

        val vehicle_ds = vehicle_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
        val vehicle_ds_schema = vehicle_ds.select(from_json($"value", vehicle_schema).as("data")).select("data.*")
        vehicle_ds_schema.printSchema()

        val vehicle_ds_unnest = vehicle_ds_schema.select("trip.*","currentStopSequence","currentStatus","timestamp","stopId")

        val trip_update_ds_unnest = trip_update_ds_schema.select($"trip.*", $"stopTimeUpdate.arrival.time".as("arrivalTime"), $"stopTimeUpdate.departure.time".as("depatureTime"), $"stopTimeUpdate.stopId")

        val trip_update_ds_unnest2 = trip_update_ds_unnest.withColumn("numOfFutureStops", size($"arrivalTime")).withColumnRenamed("stopId","futureStopIds")

        val joined_ds = trip_update_ds_unnest2.join(vehicle_ds_unnest, Seq("tripId","routeId","startTime","startDate")).withColumn("startTime",(col("startTime").cast("timestamp"))).withColumn("currentTs",from_unixtime($"timestamp".divide(1000))).drop("startDate").drop("timestamp")

        joined_ds.printSchema()

        //console
        val joined_query = joined_ds.writeStream.outputMode("complete").format("console").option("truncate", "true").outputMode(OutputMode.Append()).trigger(Trigger.ProcessingTime("10 seconds")).start()

        // write output to S3
        val joined_query_s3 = joined_ds.writeStream.outputMode("append").format("parquet").queryName("MTA").option("checkpointLocation", "/user/hadoop/checkpoint").trigger(Trigger.ProcessingTime("10 seconds")).option("path", "s3://your_s3_bucket/prefix/streaming_output/").start().awaitTermination()
    }

    // Send message to kafka
    def send(msg: String) = {
        val message = new ProducerRecord[String, String](trip_update_topic, null, s"$msg")
        msgId = msgId + 1
        producer.send(message)
    }
  }

MTASubwayTripUpdates.start
