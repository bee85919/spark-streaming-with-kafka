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
clickCntByAdId.writeStream.
  outputMode("update").
  format("console").
  start()


