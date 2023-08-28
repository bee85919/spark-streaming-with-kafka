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

outputDfJoinedClick.
  writeStream.
  outputMode("append").
  format("kafka").
  option("kafka.bootstrap.servers", "localhost:29092,localhost:39092").
  option("topic", "joined-click-event").
  option("checkpointLocation", checkpointPath).
  trigger(Trigger.ProcessingTime("5 seconds")).
  start()