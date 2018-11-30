package com.krux.starport.db.record

import org.joda.time.DateTime


case class ScheduleFailureCounter(
  pipelineId: Int,
  failureCount: Int,
  updatedAt: DateTime
)
