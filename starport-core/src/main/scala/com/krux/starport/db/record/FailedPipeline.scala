package com.krux.starport.db.record

import org.joda.time.DateTime

case class FailedPipeline(
  awsId: String,
  pipelineId: Int,
  resolved: Boolean,
  checkedTime: DateTime
)
