package com.krux.starport.db.record

import java.time.LocalDateTime


case class ScheduledPipeline(
  awsId: String,
  pipelineId: Int,
  pipelineName: String,
  scheduledStart: LocalDateTime,
  actualStart: LocalDateTime,
  deployedTime: LocalDateTime,
  status: String,
  inConsole: Boolean
)
