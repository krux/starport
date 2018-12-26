package com.krux.starport.db.record

import org.joda.time.DateTime


case class Pipeline(
  id: Option[Int],
  name: String,
  jar: String,
  `class`: String,
  isActive: Boolean,
  retention: Int,
  start: DateTime,
  period: String,  // TODO make this of type period
  end: Option[DateTime],
  nextRunTime: Option[DateTime],
  backfill: Boolean,
  owner: Option[String]
) {

  final val logPrefix = s"|PipelineId: $id|"
}
