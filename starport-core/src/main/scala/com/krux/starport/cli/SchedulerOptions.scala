package com.krux.starport.cli

import org.joda.time.DateTime


case class SchedulerOptions(
  scheduledStart: DateTime = DateTime.now,
  scheduledEnd: DateTime = DateTime.now,
  actualStart: DateTime = DateTime.now
)
