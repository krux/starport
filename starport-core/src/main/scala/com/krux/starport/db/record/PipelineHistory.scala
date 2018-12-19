package com.krux.starport.db.record

import org.joda.time.DateTime


case class PipelineHistory(
  pipelineId: Int,
  nextRunTime: Option[DateTime],
  status: String
)
