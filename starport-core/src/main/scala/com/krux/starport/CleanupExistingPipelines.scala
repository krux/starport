package com.krux.starport

import java.time.LocalDateTime

import com.codahale.metrics.MetricRegistry
import slick.jdbc.PostgresProfile.api._

import com.krux.hyperion.client.{AwsClient, AwsClientForId}
import com.krux.starport.db.record.FailedPipeline
import com.krux.starport.db.table.{FailedPipelines, Pipelines, ScheduledPipelines}
import com.krux.starport.metric.{ConstantValueGauge, MetricSettings}
import com.krux.starport.util.{AwsDataPipeline, ErrorHandler, PipelineState}


/**
 * Retain managed pipelines with finished/error status in the retention setup, and deletes all
 * other managed pipelines in managed status
 */
object CleanupExistingPipelines extends StarportActivity {

  val metrics = new MetricRegistry()

  lazy val reportingEngine: MetricSettings = conf.metricsEngine

  def activePipelineRecords(): Int = {
    logger.info("Retriving active pipelines...")

    val query = Pipelines()
      .filter(_.isActive).size

    val result = db.run(query.result).waitForResult

    logger.info(s"Retrieved ${result} pending pipelines")

    result
  }

  private def updateToNotInConsole(awsId: String) = db
    .run(
      ScheduledPipelines()
        .filter(_.awsId === awsId)
        .map(_.inConsole)
        .update(false)
    )
    .waitForResult

  private def deletePipelineAndUpdateDB(awsId: String): Unit = {
    // delete the pipeline from console then update the field in the database
    val clientForSp = AwsClientForId(AwsClient.getClient(), Set(awsId), conf.maxRetry)
    clientForSp.deletePipelines()
    updateToNotInConsole(awsId)
  }

  def run() = {

    logger.info(s"Getting list of in console pipelines to delete...")

    // get the list of pipelines to be checked this time
    val inConsolePipelines = db.run(ScheduledPipelines().filter(_.inConsole).result).waitForResult

    logger.info(s"Retrieved ${inConsolePipelines.size} in console pipelines.")
    metrics.register(
      "gauges.in_console_pipelines_count",
      new ConstantValueGauge(inConsolePipelines.size)
    )

    val deleteCounter = metrics.counter("counters.pipeline_deleted")

    // delete the in console pipelines that no longer need to be there (keep the most x recent
    // success/error piplines, where x is the retention value in db)
    inConsolePipelines.groupBy(_.pipelineId).par.foreach { case (pipelineId, scheduledPipelines) =>

      val pipelineRecord = db.run(Pipelines().filter(_.id === pipelineId).take(1).result)
        .waitForResult
        .head

      // TODO refactor the try
      try {

        // TODO probably better to just use a different logger
        val logPrefix = s"|PipelineId: ${pipelineId}|"
        logger.info(s"$logPrefix has ${scheduledPipelines.size} in console pipelines in DB.")

        val pipelineStatuses = AwsDataPipeline.describePipeline(scheduledPipelines.map(_.awsId): _*)

        val awsManagedKeySet = pipelineStatuses.keySet
        logger.info(s"$logPrefix AWS contains ${awsManagedKeySet.size}")
        val inStarportButNotInAws = scheduledPipelines.map(_.awsId).toSet -- awsManagedKeySet
        logger.info(s"$logPrefix updating the inConsole status to false for ${inStarportButNotInAws.size} entries")
        inStarportButNotInAws.foreach(updateToNotInConsole)

        val finishedPipelines = scheduledPipelines.filter { p =>
          pipelineStatuses.get(p.awsId).flatMap(_.pipelineState) == Some(PipelineState.FINISHED)
        }

        logger.info(s"$logPrefix has ${finishedPipelines.size} finished pipelines.")

        val (failedPipelines, healthyPipelines) = finishedPipelines.partition { p =>
          pipelineStatuses.get(p.awsId).flatMap(_.healthStatus) == Some("ERROR")
        }

        logger.info(s"$logPrefix has ${failedPipelines.size} failed pipelines.")
        logger.info(s"$logPrefix has ${healthyPipelines.size} healthy pipelines.")

        // delete the extra healthy pipelines
        healthyPipelines
          .groupBy(_.actualStart)
          .toSeq
          .sortBy(_._1)(Ordering[LocalDateTime].reverse)
          .drop(pipelineRecord.retention)
          .flatMap(_._2)
          .foreach { sp =>
            logger.info(s"$logPrefix ask to delete ${sp.awsId}.")
            deletePipelineAndUpdateDB(sp.awsId)
            deleteCounter.inc()
          }

        val (toBeKeptFailedMap, toBeDeletedFailedMap) = failedPipelines
          .groupBy(_.actualStart)
          .toSeq
          .sortBy(_._1)(Ordering[LocalDateTime].reverse)
          .splitAt(pipelineRecord.retention)

        val toBeKeptFailed = toBeKeptFailedMap.flatMap(_._2)
        val toBeDeletedFailed = toBeDeletedFailedMap.flatMap(_._2)

        toBeDeletedFailed.foreach(sp => deletePipelineAndUpdateDB(sp.awsId))

        // insert the error pipelines in the database
        // TODO refactor
        toBeKeptFailed
          .filter { sp =>
            db.run(FailedPipelines().filter(_.awsId === sp.awsId).result)
              .waitForResult
              .size == 0
          }
          .foreach { sp =>
            val failedPipeline = FailedPipeline(sp.awsId, sp.pipelineId, false, currentTimeUTC().toLocalDateTime)
            logger.info(s"insert ${failedPipeline.awsId} to failed pipelines")
            db.run(DBIO.seq(FailedPipelines() += failedPipeline)).waitForResult
          }
      } catch {
        case e: Exception =>
          ErrorHandler.cleanupActivityFailed(pipelineRecord, e.getStackTrace)
      }

    }

    // Report the total pipeline count in the end. (This is mainly for stats, and not vital for
    // starport operation)
    val totalPipelineCount = AwsDataPipeline.listPipelineIds().size
    metrics.register(
      "gauges.total_pipeline_count",
      new ConstantValueGauge(totalPipelineCount)
    )

  }

  def main(args: Array[String]): Unit = {

    val reporter = reportingEngine.getReporter(metrics)

    val start = System.nanoTime
    try {
      run()

      val timeSpan = (System.nanoTime - start) / 1E9
      logger.info(s"All pipelines cleaned up in $timeSpan seconds")

      val numActivePipelines = activePipelineRecords()
      metrics.register(
        "gauges.active_pipeline_count", new ConstantValueGauge(numActivePipelines)
      )
    } finally {
      reporter.report()
      reporter.close()
    }
  }

}
