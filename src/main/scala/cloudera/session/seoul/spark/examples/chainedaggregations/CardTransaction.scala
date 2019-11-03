package cloudera.session.seoul.spark.examples.chainedaggregations

import java.util.UUID
import java.util.concurrent.TimeUnit

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
