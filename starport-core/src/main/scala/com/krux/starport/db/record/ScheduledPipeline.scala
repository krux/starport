package com.krux.starport.db.record

import org.joda.time.DateTime


case class ScheduledPipeline(
  awsId: String,
  pipelineId: Int,
  pipelineName: String,
  scheduledStart: DateTime,
  actualStart: DateTime,
  deployedTime: DateTime,
  status: String,
  inConsole: Boolean
)
