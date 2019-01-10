package com.krux.starport.db.tool

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.WaitForIt
import com.krux.starport.util.{PipelineProgressHelper, ProgressStatus}
import com.krux.starport.Logging
import com.krux.starport.db.table.PipelineProgresses


object MarkPipelineAsFinished extends WaitForIt with Logging {

  implicit val starportSettings = StarportSettings()

  lazy val db = starportSettings.jdbc.db

  def showStatus(pipelineId: Int): Unit = {
    val query = PipelineProgresses()
      .filter(_.pipelineId === pipelineId)
      .map(_.progress)
    val progress = db.run(query.result).waitForResult.headOption
    logger.info(s"Pipeline $pipelineId progress -> ${progress.getOrElse("Not Found")}")
  }

  def markAsSuccess(pipelineId: Int): Unit = {
    new PipelineProgressHelper().insertOrUpdatePipelineProgress(pipelineId, ProgressStatus.Success)
    logger.info(s"Marked pipeline $pipelineId as SUCCESS")
  }

  def main(args: Array[String]): Unit = args.toList match {
    case "show" :: pipelineIds => pipelineIds.foreach(pid => showStatus(pid.toInt))
    case "mark" :: pipelineIds => pipelineIds.foreach(pid => markAsSuccess(pid.toInt))
    case _ => println("Illegal arguments")
  }
}
