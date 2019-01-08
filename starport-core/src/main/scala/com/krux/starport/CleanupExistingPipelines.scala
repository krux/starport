package com.krux.starport

import com.codahale.metrics.MetricRegistry
import com.github.nscala_time.time.Imports._
import slick.jdbc.PostgresProfile.api._

import com.krux.hyperion.client.{AwsClient, AwsClientForId}
import com.krux.starport.db.record.FailedPipeline
import com.krux.starport.db.table.{FailedPipelines, Pipelines, ScheduledPipelines}
import com.krux.starport.metric.{ConstantValueGauge, MetricSettings}
import com.krux.starport.util.{AwsDataPipeline, ErrorHandler, ProgressStatus, PipelineProgressHelper, PipelineState}


/**
 * Retain managed pipelines with finished/error status in the retention setup, and deletes all
 * other managed pipelines in managed status
 */
object CleanupExistingPipelines extends StarportActivity {

  val taskName = "CleanupExistingPipelines"

  val metrics = new MetricRegistry()

  def activePipelineRecords(): Int = {
    logger.info("Retrieving active pipelines...")

    val query = Pipelines()
      .filter(_.isActive).size

    val result = db.run(query.result).waitForResult

    logger.info(s"Retrieved ${result} pending pipelines")

    result
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

    // 1) delete the in console pipelines that no longer need to be there (keep the most x recent
    //    success/error piplines, where x is the retention value in db)
    // 2) mark pipeline success / failure status in pipeline_progresses
    inConsolePipelines.groupBy(_.pipelineId).par.foreach { case (pipelineId, scheduledPipelines) =>

      val pipelineRecord = db.run(Pipelines().filter(_.id === pipelineId).take(1).result)
        .waitForResult
        .head

      // TODO refactor the try
      try {

        logger.info(s"${pipelineRecord.id} has ${scheduledPipelines.size} in console pipelines.")

        val pipelineStatuses = AwsDataPipeline.describePipeline(scheduledPipelines.map(_.awsId): _*)

        val finishedPipelines = scheduledPipelines.filter { p =>
          pipelineStatuses.get(p.awsId).flatMap(_.pipelineState) == Some(PipelineState.FINISHED)
        }

        logger.info(s"Pipeline ${pipelineRecord.id} has ${finishedPipelines.size} finished pipelines.")

        val (failedPipelines, healthyPipelines) = finishedPipelines.partition { p =>
          pipelineStatuses.get(p.awsId).flatMap(_.healthStatus) == Some("ERROR")
        }

        logger.info(s"Pipeline ${pipelineRecord.id} has ${failedPipelines.size} failed pipelines.")
        logger.info(s"Pipeline ${pipelineRecord.id} has ${healthyPipelines.size} healthy pipelines.")

        import scala.concurrent.ExecutionContext.Implicits.global

        val pipelineProgressHelper = new PipelineProgressHelper()
        pipelineProgressHelper.insertOrUpdatePipelineProgress(healthyPipelines.map(_.pipelineId).toSet, ProgressStatus.SUCCESS)
        pipelineProgressHelper.insertOrUpdatePipelineProgress(failedPipelines.map(_.pipelineId).toSet, ProgressStatus.FAILED)

        def deletePipelineAndUpdateDB(awsId: String): Unit = {
          // delete the pipeline from console then update the field in the database
          val clientForSp = AwsClientForId(AwsClient.getClient(), Set(awsId), conf.maxRetry)

          clientForSp.deletePipelines()
          val updateStatusQuery = ScheduledPipelines()
            .filter(_.awsId === awsId)
            .map(_.inConsole)
            .update(false)
          db.run(updateStatusQuery).waitForResult
        }

        // delete the extra healthy pipelines
        healthyPipelines
          .groupBy(_.actualStart)
          .toSeq
          .sortBy(_._1)(Ordering[DateTime].reverse)
          .drop(pipelineRecord.retention)
          .flatMap(_._2)
          .foreach { sp =>
            logger.info(s"Pipeline ${pipelineRecord.id} ask to delete ${sp.awsId}.")
            deletePipelineAndUpdateDB(sp.awsId)
            deleteCounter.inc()
          }

        val (toBeKeptFailedMap, toBeDeletedFailedMap) = failedPipelines
          .groupBy(_.actualStart)
          .toSeq
          .sortBy(_._1)(Ordering[DateTime].reverse)
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
            val failedPipeline = FailedPipeline(sp.awsId, sp.pipelineId, false, DateTime.now)
            logger.info(s"insert ${failedPipeline.awsId} to failed pipelines")
            db.run(DBIO.seq(FailedPipelines() += failedPipeline)).waitForResult
          }

      } catch {
        case e: Exception =>
          e.printStackTrace()
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

    val reporter = MetricSettings.getReporter(conf.metricSettings, metrics)

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
    }
  }

}
