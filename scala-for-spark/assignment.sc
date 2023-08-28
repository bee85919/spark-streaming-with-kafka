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

cntByImp.writeStream.
  outputMode("update").
  format("console").
  start()