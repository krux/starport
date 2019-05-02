package com.krux.starport

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat => JodaDateTimeFormat}
import slick.jdbc.PostgresProfile.api._

import com.krux.starport.cli.{CleanupNonStarportOptionParser, CleanupNonStarportOptions}
import com.krux.starport.db.table.ScheduledPipelines
import com.krux.starport.util.{AwsDataPipeline, PipelineStatus, PipelineState}

object CleanupNonStarportPipelines extends StarportActivity {
  final val AwsDateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss"
  final val taskName = "CleanupNonStarportPipelines"

  def nonStarportPipelineIds(
      pipelineState: PipelineState.State,
      cutoffDate: DateTime
    ): Set[String] = {

    logger.info(s"Getting list of old ${pipelineState} non-Starport pipelines from AWS to delete...")
    val dateTimeFormatter = JodaDateTimeFormat.forPattern(AwsDateTimeFormat)
    val inConsoleStarportScheduledPipelineIds = db.run(
        ScheduledPipelines()
          .filter(_.inConsole)
          .distinctOn(_.awsId)
          .result
      ).waitForResult.map(_.awsId).toSet

    logger.info(s"Retrieved ${inConsoleStarportScheduledPipelineIds.size} in console pipelines from Starport DB.")

    def shouldPipelineBeDeleted(pipelineStatus: Option[PipelineStatus]): Boolean = {
      if (pipelineStatus.flatMap(_.pipelineState) != Some(pipelineState))
        return false

      pipelineStatus.flatMap(_.creationTime) match {
        case Some(creationTime) => dateTimeFormatter.parseDateTime(creationTime) < cutoffDate.withTimeAtStartOfDay
        case None => false
      }
    }

    val nonStarportPipelineIds = AwsDataPipeline.listPipelineIds() -- inConsoleStarportScheduledPipelineIds
    val pipelineStatuses = AwsDataPipeline.describePipeline(nonStarportPipelineIds.toSeq: _*)
    nonStarportPipelineIds.filter { pId => shouldPipelineBeDeleted(pipelineStatuses.get(pId)) }
  }

  def deletePipelines(ids: Set[String], dryRun: Boolean): Unit = {
    logger.info(s"Deleting ${ids.size} pipelines from AWS.")
    if (dryRun)
      println(s"Dry run option is enabled. Otherwise, these AWS pipeline IDs would be deleted:\n${ids.mkString("\n")}")
    else
      AwsDataPipeline.deletePipelines(ids)
  }

  def run(options: CleanupNonStarportOptions) = {
    logger.info(s"run with options: $options")

    val ids = nonStarportPipelineIds(options.pipelineState, options.cutoffDate)
    logger.info(s"${ids.size} pipelines found")

    deletePipelines(ids, options.dryRun)
  }

  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    CleanupNonStarportOptionParser.parse(args) match {
      case Some(options) => run(options)
      case None => ErrorExit.invalidCommandlineArguments(logger)
    }
    val timeSpan = (System.nanoTime - start) / 1E9
    logger.info(s"All old and FINISHED non-Starport pipelines cleaned up in $timeSpan seconds")
  }
}
