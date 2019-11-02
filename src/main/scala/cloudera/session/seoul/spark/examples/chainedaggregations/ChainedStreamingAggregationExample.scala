package cloudera.session.seoul.spark.examples.chainedaggregations

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

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
      .groupBy(window(col("timestamp"), "10 minutes"), col("province"), col("city"), col("amountGroup"))
      .agg(count("*").as("count"), sum("amount").as("sum"))
      .selectExpr("window.start AS window_start", "window.end AS window_end", "province", "city", "amountGroup", "count", "sum")
      .writeStream
      .format("json")
      .outputMode(OutputMode.Append)
      .option("path", OUTPUT_PATH)
      .option("checkpointLocation", CHECKPOINT_PATH)
      .start()

    provideInputs(input)

    query.awaitTermination()
  }

  private def provideInputs(input: MemoryStream[CardTransaction]): Unit = {
    // 0 ~ 10 mins (first window)
    input.addData(CardTransaction("Seoul", "Seoul", 9000, 0))
    input.addData(CardTransaction("Seoul", "Seoul", 13000, 1))
    input.addData(CardTransaction("Seoul", "Seoul", 150000, 5))
    input.addData(CardTransaction("Seoul", "Seoul", 2000000, 9))

    // 0 ~ 30 mins (various windows in first group, but same window in second group)

    // below twos are in a same window in first group
    input.addData(CardTransaction("Gyeonggi", "Suji", 12000, 9))
    input.addData(CardTransaction("Gyeonggi", "Suji", 27000, 5))
    // below twos are not a same window in first group
    input.addData(CardTransaction("Gyeonggi", "Suji", 156000, 15))
    input.addData(CardTransaction("Gyeonggi", "Suji", 900000, 27))

    input.addData(CardTransaction("Gyeonggi", "Seongnam", 130000, 15))
    input.addData(CardTransaction("Gyeonggi", "Seongnam", 1100000, 25))
    input.addData(CardTransaction("Gyeonggi", "Bucheon", 300000, 5))
    input.addData(CardTransaction("Gyeonggi", "Bucheon", 20000, 8))

    // 31 ~ 60 mins (different windows than above)

    // below twos are in a same window in first group
    input.addData(CardTransaction("Gyeonggi", "Suji", 12000, 31))
    input.addData(CardTransaction("Gyeonggi", "Suji", 27000, 35))
    // below twos are not a same window in first group
    input.addData(CardTransaction("Gyeonggi", "Suji", 156000, 35))
    input.addData(CardTransaction("Gyeonggi", "Suji", 900000, 55))

    // force emit watermark to see result immediately (2hr)
    // there's no way to only emit watermark in Spark Structured Streaming...
    // add a dummy data which has ts as 2hr
    input.addData(CardTransaction("dummy", "dummy", 0, 2 * 60))

    // to advance watermark in second query, we should push out dummy data to the
    // output of first query... you may feel how much the final output will be delayed
    input.addData(CardTransaction("dummy", "dummy", 0, 2 * 60 + 20))
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
      .select(col("window_start").as("timestamp"), col("province"), col("amountGroup"), col("count"), col("sum"))
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

case class CardTransaction(
    // UUID cannot be used naturally unless custom Encoder is implemented
    transactionId: String,
    province: String,
    city: String,
    amount: Long,
    timestamp: Long)


object TransactionAmountGroup extends Enumeration {
  // A: ~ 9999
  // B: 10000 ~ 99999
  // C: 100000 ~ 999999
  // D: 1000000 ~
  val A, B, C, D = Value

  def group(amount: Long): TransactionAmountGroup.Value = {
    if (amount < 10000) A
    else if (amount >= 10000 && amount < 100000) B
    else if (amount >= 100000 && amount < 1000000) C
    else D
  }
}

object CardTransaction {
  def apply(province: String, city: String, amount: Long, tsMins: Long): CardTransaction = {
    CardTransaction(UUID.randomUUID().toString, province, city, amount, minutesToMillis(tsMins))
  }

  private def minutesToMillis(min: Long): Long = {
    TimeUnit.MINUTES.toMillis(min)
  }
}
