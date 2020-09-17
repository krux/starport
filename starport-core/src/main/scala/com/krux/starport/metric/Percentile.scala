package com.krux.starport.metric

sealed trait Percentile {

  def quantile: Double
  def description: String

}

case object P50 extends Percentile {

  val quantile: Double = 0.50
  val description = "50%"

}

case object P75 extends Percentile {

  val quantile: Double = 0.75
  val description = "75%"

}

case object P95 extends Percentile {

  val quantile: Double = 0.95
  val description = "95%"

}

case object P98 extends Percentile {

  val quantile: Double = 0.98
  val description = "98%"

}

case object P99 extends Percentile {

  val quantile: Double = 0.99
  val description = "99%"

}

case object P995 extends Percentile {

  val quantile: Double = 0.995
  val description = "99.5%"

}

case object P999 extends Percentile {

  val quantile: Double = 0.999
  val description = "99.9%"

}
