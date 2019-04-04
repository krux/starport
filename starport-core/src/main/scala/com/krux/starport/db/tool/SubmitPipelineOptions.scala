package com.krux.starport.db.tool

import com.github.nscala_time.time.Imports._

import com.krux.hyperion.expression.Duration

case class SubmitPipelineOptions(
  jar: String = "",
  pipelineObject: String = "",
  startNow: Boolean = false,
  update: Boolean = false,
  noBackfill: Boolean = false,
  baseDir: Option[String] = None,
  cleanUp: Boolean = false,
  schedule: Option[DateTime] = None,
  frequency: Option[Duration] = None,
  enable: Option[Boolean] = None,
  owner: Option[String] = None,
  dryRun: Boolean = false ) {
    def backfill = !noBackfill
}

