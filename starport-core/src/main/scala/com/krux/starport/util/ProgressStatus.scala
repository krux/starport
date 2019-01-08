package com.krux.starport.util

object ProgressStatus extends Enumeration {

  type Progress = Value

  val SUCCESS = Value("success")
  val FAILED = Value("failed")
  val WAITING = Value("waiting")
  val RUNNING = Value("running")
}
