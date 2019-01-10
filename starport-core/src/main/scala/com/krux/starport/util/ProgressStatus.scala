package com.krux.starport.util

object ProgressStatus extends Enumeration {

  type Progress = Value

  final val Success = Value("success")
  final val Failed = Value("failed")
  final val Waiting = Value("waiting")
  final val Running = Value("running")
}
