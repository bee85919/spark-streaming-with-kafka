/*
확인 코드 (iTerm)
$KAFKA_HOME/bin/kafka-console-consumer.sh --topic impression-event --from-beginning --bootstrap-server localhost:29092,localhost:39092
$KAFKA_HOME/bin/kafka-console-consumer.sh --topic click-event --from-beginning --bootstrap-server localhost:29092,localhost:39092
*/


/*
아래의 코드는 아래의 spark-shell+kafka에서 진행
spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
*/


// impression-event Dataset으로 읽기
import org.apache.spark.sql.functions._
val dfImp = spark.
  readStream.
  format("kafka").
  option("kafka.bootstrap.servers", "localhost:29092,localhost:39092").
  option("subscribe", "impression-event").
  load()


// dfImp 확인
dfImp.printSchema


// 스키마
import org.apache.spark.sql.types._
val schemaImpressionEvent = StructType(Array(
  StructField("impId", StringType, false),
  StructField("requestId", StringType, true),
  StructField("adId", StringType, true),
  StructField("userId", StringType, true),
  StructField("deviceId", StringType, true),
  StructField("inventoryId", StringType, true),
  StructField("timestamp", LongType, false)
))


// 스트림
val streamDfImp = dfImp.
  withColumn("value", from_json(col("value").cast("string"), schemaImpressionEvent)).
  select(col("value.*")).
  withColumn(
    "timestamp",
    (col("timestamp")/1000).cast(TimestampType)
  )


// 스트림 스키마 확인
streamDfImp.printSchema


// 확인
val cntByImp = streamDfImp.groupBy(col("impId"), col("timestamp")).count()

//cntByImp.writeStream.
//  outputMode("update").
//  format("console").
//  start()


// click-event Dataset으로 읽기
import org.apache.spark.sql.functions._
val dfClick = spark.
  readStream.
  format("kafka").
  option("kafka.bootstrap.servers", "localhost:29092,localhost:39092").
  option("subscribe", "click-event").
  load()


// dfClick 확인
dfClick.printSchema


// click-event Dataset으로 읽기
import org.apache.spark.sql.functions._
val dfClick = spark.
  readStream.
  format("kafka").
  option("kafka.bootstrap.servers", "localhost:29092,localhost:39092").
  option("subscribe", "click-event").
  load()


// 클릭 이벤트 스키마
import org.apache.spark.sql.types._
val schemaClick = StructType(Array(
  StructField("impId", StringType, false),
  StructField("clickUrl", StringType, true),
  StructField("timestamp", LongType, false)
))


// 스트림
val streamDfClick = dfClick.
  withColumn("value", from_json(col("value").cast("string"), schemaClick)).
  select(col("value.*")).
  withColumn(
    "timestamp",
    (col("timestamp")/1000).cast(TimestampType)
  )


// 확인
streamDfClick.printSchema


// 테스트
val cntByClick = streamDfClick.groupBy(col("impId"), col("timestamp")).count()

//cntByClick.writeStream.
//  outputMode("update").
//  format("console").
//  start()


// impression-event 와 click-event join하기
import spark.implicits._
val streamDfJoinedClick = streamDfImp.withColumnRenamed("timestamp", "impTimestamp").
  withWatermark("impTimestamp", "60 seconds").
  dropDuplicates("impId", "impTimestamp").
  join(
    streamDfClick.
      withWatermark("timestamp", "60 seconds")
      dropDuplicates("impId", "timestamp")
    ,
    "impId")


// streamDfJoinedClick 확인
streamDfJoinedClick.printSchema


// 테스트
val cntByJoinedClick = streamDfJoinedClick.withWatermark("timestamp", "120 seconds").
  groupBy(col("impId"), col("adId"), col("timestamp")).count()

//cntByJoinedClick.writeStream.
//  outputMode("append").
//  format("console").
//  start()


// 데이터 변환
val outputDfJoinedClick = streamDfJoinedClick.
  select(col("adId"), to_json(struct($"*"))).
  toDF("key", "value")


// 카프카 전송
val dataPath = System.getenv("SPARK_DATA")
val checkpointPath = s"${dataPath}/streaming/checkpoint/write-joined-click-event-1"

import org.apache.spark.sql.streaming.Trigger

//outputDfJoinedClick.
//  writeStream.
//  outputMode("append").
//  format("kafka").
//  option("kafka.bootstrap.servers", "localhost:29092,localhost:39092").
//  option("topic", "joined-click-event").
//  option("checkpointLocation", checkpointPath).
//  trigger(Trigger.ProcessingTime("5 seconds")).
//  start()

// adId 기준으로 joined click event 를 count 하기
import org.apache.spark.sql.functions._
val dfJoinedClick = spark.
  readStream.
  format("kafka").
  option("kafka.bootstrap.servers", "localhost:29092,localhost:39092").
  option("subscribe", "joined-click-event").
  load()


// dfJoinedClick 확인
dfJoinedClick.printSchema


// 스키마
import org.apache.spark.sql.types._

val schemaJoinedClickEvent = StructType(Array(
  StructField("impId", StringType, false),
  StructField("requestId", StringType, true),
  StructField("adId", StringType, true),
  StructField("userId", StringType, true),
  StructField("deviceId", StringType, true),
  StructField("inventoryId", StringType, true),
  StructField("clickUrl", StringType, true),
  StructField("impTimestamp", TimestampType, false),
  StructField("timestamp", TimestampType, false)
))


// 스트림
val streamDfJoinedClick = dfJoinedClick.
  withColumn("value", from_json(col("value").cast("string"), schemaJoinedClickEvent)).
  select(col("value.*"))


// 스트림 스키마
streamDfJoinedClick.printSchema


// adId 기준으로 joined click count
import spark.implicits._

val clickCntByAdId = streamDfJoinedClick.
  withWatermark("timestamp", "120 seconds").
  groupBy(
    window($"timestamp", "2 minutes"),
    col("adId")
  ).
  count().
  withColumn("time", lit(""))


// 데이터가 제대로 들어오는지 확인
//clickCntByAdId.writeStream.
//  outputMode("update").
//  format("console").
//  start()

// csv 파일로 내보낸다
val dataPath = System.getenv("SPARK_DATA")
val outputPath = s"${dataPath}/streaming/output/clickCntByAdId-2"
val checkpointPath = s"${dataPath}/streaming/checkpoint/clickCntByAdId-1"

import org.apache.spark.sql.streaming.Trigger

clickCntByAdId.
  writeStream.
  outputMode("append").
  format("parquet").
  trigger(Trigger.ProcessingTime("2 minutes")).
  option("checkpointLocation", checkpointPath).
  partitionBy("window").
  start(outputPath)

