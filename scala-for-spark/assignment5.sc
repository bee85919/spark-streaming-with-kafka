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