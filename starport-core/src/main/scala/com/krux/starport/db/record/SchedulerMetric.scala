package com.krux.starport.db.record

import java.time.LocalDateTime


case class SchedulerMetric(
  startTime: LocalDateTime,
  piplineCount: Option[Int] = None,
  endTime: Option[LocalDateTime] = None
)
