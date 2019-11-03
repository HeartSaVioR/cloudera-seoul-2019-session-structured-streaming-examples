package cloudera.session.seoul.spark.examples.chainedaggregations

import org.apache.spark.sql.execution.streaming.MemoryStream

object CardTransactionSource {
  def provideInputs(input: MemoryStream[CardTransaction]): Unit = {
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
