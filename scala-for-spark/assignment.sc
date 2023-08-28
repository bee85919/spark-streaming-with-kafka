/*
확인 코드 (iTerm)
$KAFKA_HOME/bin/kafka-console-consumer.sh --topic impression-event --from-beginning --bootstrap-server localhost:29092,localhost:39092
$KAFKA_HOME/bin/kafka-console-consumer.sh --topic click-event --from-beginning --bootstrap-server localhost:29092,localhost:39092
*/


import org.apache.spark.sql.SparkSession

object Main extends App {
  val sparkHome = System.getenv("SPARK_HOME")
  val logDir = s"file:${sparkHome}/event"
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkExample")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", logDir)
    .getOrCreate();
}


// impression-event Dataset으로 읽기
import org.apache.spark.sql.functions._
val dfImp = spark.
  readStream.
  format("kafka").
  option("kafka.bootstrap.servers", "localhost:29092,localhost:39092").
  option("subscribe", "impression-event").
  load()


dfImp.printSchema
