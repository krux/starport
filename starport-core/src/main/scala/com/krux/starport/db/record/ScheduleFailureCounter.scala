package com.krux.starport.db.record

import java.time.LocalDateTime


case class ScheduleFailureCounter(
  pipelineId: Int,
  failureCount: Int,
  updatedAt: LocalDateTime
)
