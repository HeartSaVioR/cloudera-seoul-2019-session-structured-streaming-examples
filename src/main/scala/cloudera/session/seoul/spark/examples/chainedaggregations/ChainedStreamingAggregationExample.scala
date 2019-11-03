package cloudera.session.seoul.spark.examples.chainedaggregations

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}

// To run the query, you need to ensure all 4 directories don't exist.
// 'rm -rf /tmp/chained-streaming-agg'
// You can check the intermediate output from `/tmp/chained-streaming-agg/query1/output`
// and final output from `/tmp/chained-streaming-agg/query2/output`

object ChainedStreamingAggregationExampleFirstQuery {
  val CHECKPOINT_PATH = "/tmp/chained-streaming-agg/query1/checkpoint"
  val OUTPUT_PATH = "/tmp/chained-streaming-agg/query1/output"

  def main(args: Array[String]): Unit = {
    val appName = "chained-streaming-aggregation-query-1"

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    implicit val sqlContext = spark.sqlContext

    val input = MemoryStream[CardTransaction]

    val inputDf = input.toDS()
      .map(ct => (ct.transactionId, ct.province, ct.city, TransactionAmountGroup.group(ct.amount).toString,
        ct.amount, new Timestamp(ct.timestamp)))
      .toDF("transactionId", "province", "city", "amountGroup", "amount", "timestamp")
      .withWatermark("timestamp", "10 minutes")

    // Note that this query MUST ENSURE "end-to-end" exactly once - otherwise intermediate output could have
    // duplicated rows which mess up final output unless the second query deduplicates them properly.

    val query = inputDf
      .groupBy(window(col("timestamp"), "10 minutes"), col("province"),
        col("city"), col("amountGroup"))
      .agg(count("*").as("count"), sum("amount").as("sum"))
      .selectExpr("window.start AS window_start", "window.end AS window_end", "province", "city", "amountGroup", "count", "sum")
      .writeStream
      .format("json")
      .outputMode(OutputMode.Append)
      .option("path", OUTPUT_PATH)
      .option("checkpointLocation", CHECKPOINT_PATH)
      .start()

    CardTransactionSource.provideInputs(input)

    query.awaitTermination()
  }
}

object ChainedStreamingAggregationExampleSecondQuery {
  val CHECKPOINT_PATH = "/tmp/chained-streaming-agg/query2/checkpoint"
  val OUTPUT_PATH = "/tmp/chained-streaming-agg/query2/output"

  def main(args: Array[String]): Unit = {
    val appName = "chained-streaming-aggregation-query-2"

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[2]")
      .getOrCreate()

    // "window.start AS window_start", "window.end AS window_end", "province", "city", "amountGroup", "count", "sum"
    val schema = new StructType()
      .add("window_start", TimestampType)
      .add("window_end", TimestampType)
      .add("province", StringType)
      .add("city", StringType)
      .add("amountGroup", StringType)
      .add("count", LongType)
      .add("sum", LongType)

    val inputDf = spark
      .readStream
      .format("json")
      .option("path", ChainedStreamingAggregationExampleFirstQuery.OUTPUT_PATH)
      .schema(schema)
      .load()

    val query = inputDf
      .select(col("window_start").as("timestamp"), col("province"),
        col("amountGroup"), col("count"), col("sum"))
      .withWatermark("timestamp", "0 minute")
      .groupBy(window(col("timestamp"), "30 minutes"), col("province"), col("amountGroup"))
      .agg(sum("count").as("count"), sum("sum").as("sum"))
      .selectExpr("window.start AS window_start", "window.end AS window_end", "province", "amountGroup", "count", "sum")
      .writeStream
      .format("json")
      .outputMode(OutputMode.Append)
      .option("path", OUTPUT_PATH)
      .option("checkpointLocation", CHECKPOINT_PATH)
      .start()

    query.awaitTermination()
  }
}
