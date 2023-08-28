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

cntByClick.writeStream.
  outputMode("update").
  format("console").
  start()