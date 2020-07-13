package com.krux.starport.db.tool

import java.time.LocalDateTime

import com.krux.hyperion.expression.Duration

case class SubmitPipelineOptions(
  jar: String = "",
  pipelineObject: String = "",
  startNow: Boolean = false,
  update: Boolean = false,
  updateJarOnly: Boolean = false,
  noBackfill: Boolean = false,
  baseDir: Option[String] = None,
  cleanUp: Boolean = false,
  schedule: Option[LocalDateTime] = None,
  frequency: Option[Duration] = None,
  enable: Option[Boolean] = None,
  owner: Option[String] = None,
  dryRun: Boolean = false
) {

  def backfill = !noBackfill

}
