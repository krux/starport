package com.krux.starport.db.table

import slick.jdbc.PostgresProfile.api._

trait Schema {

  final val schema =
    Pipelines().schema ++
    ScheduledPipelines().schema ++
    FailedPipelines().schema ++
    SchedulerMetrics().schema ++
    ScheduleFailureCounters().schema

}

object Schema extends Schema
