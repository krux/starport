package com.krux.starport.db.record

import java.time.LocalDateTime

case class FailedPipeline(
  awsId: String,
  pipelineId: Int,
  resolved: Boolean,
  checkedTime: LocalDateTime
)
