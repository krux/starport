package com.krux.starport.util

object HealthStatus extends Enumeration {

  type HealthStatus = Value

  val SUCCESS = Value("success")
  val FAILED = Value("failed")
  val WAITING = Value("waiting")

}
