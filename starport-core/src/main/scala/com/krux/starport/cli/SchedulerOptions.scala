package com.krux.starport.cli

import java.time.LocalDateTime

import com.krux.starport.util.DateTimeFunctions


case class SchedulerOptions(
  scheduledStart: LocalDateTime = DateTimeFunctions.currentTimeUTC().toLocalDateTime,
  scheduledEnd: LocalDateTime = DateTimeFunctions.currentTimeUTC().toLocalDateTime,
  actualStart: LocalDateTime = DateTimeFunctions.currentTimeUTC().toLocalDateTime
)
