package com.krux.starport

import java.util.concurrent.{ForkJoinPool, TimeUnit}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.ExecutionContext.Implicits.global
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.nscala_time.time.Imports._
import slick.jdbc.PostgresProfile.api._
import com.krux.hyperion.expression.{Duration => HDuration}
import com.krux.starport.cli.{SchedulerOptionParser, SchedulerOptions}
import com.krux.starport.db.record.{Pipeline, ScheduledPipeline, SchedulerMetric}
import com.krux.starport.db.table.{Pipelines, ScheduleFailureCounters, ScheduledPipelines, SchedulerMetrics}
import com.krux.starport.dispatcher.TaskDispatcher
import com.krux.starport.dispatcher.impl.TaskDispatcherImpl
import com.krux.starport.metric.{ConstantValueGauge, MetricSettings, SimpleTimerGauge}
import com.krux.starport.util.{ErrorHandler, S3FileHandler}

object StartScheduledPipelines extends StarportActivity {

  val metrics = new MetricRegistry()

  lazy val reportingEngine: MetricSettings = conf.metricsEngine

  val scheduleTimer = metrics.timer("timers.pipeline_scheduling_time")

  val extraEnvs = conf.extraEnvs.toSeq

  /**
   * return a map of remote jar to local jar
   */
  def getLocalJars(pipelineModels: Seq[Pipeline]): Map[String, String] = pipelineModels
    .map(_.jar)
    .toSet[String]
    .map { remoteFile =>
      val localFile = S3FileHandler.getFileFromS3(remoteFile)
      remoteFile -> localFile.getAbsolutePath
    }
    .toMap

  def pendingPipelineRecords(scheduledEnd: DateTime): Seq[Pipeline] = {
    logger.info("Retriving pending pipelines..")

    // get all jobs to be scheduled
    val query = Pipelines()
      .filter { p =>
        p.nextRunTime < scheduledEnd &&
        (p.end.isEmpty || p.end > p.nextRunTime) &&
        p.isActive
      }
      .sortBy(_.nextRunTime.asc)
      .take(conf.maxPipelines)

    val result = db.run(query.result).waitForResult

    logger.info(s"Retrieved ${result.size} pending pipelines")

    result
  }

  private def updateScheduledPipelines(scheduledPipelines: Seq[ScheduledPipeline]) = {
    scheduledPipelines.isEmpty match {
      case true => ()
      case false =>
        val insertAction = DBIO.seq(ScheduledPipelines() ++= scheduledPipelines)
        db.run(insertAction).waitForResult
    }
  }

  private def updateNextRunTime(pipelineRecord: Pipeline, options: SchedulerOptions) = {
    // update the next runtime in the database
    val newNextRunTime = nextRunTime(pipelineRecord.nextRunTime.get, HDuration(pipelineRecord.period), options.scheduledEnd)
    val updateQuery = Pipelines().filter(_.id === pipelineRecord.id).map(_.nextRunTime)
    logger.debug(s"Update with query ${updateQuery.updateStatement}")
    val updateAction = updateQuery.update(Some(newNextRunTime))
    db.run(updateAction).waitForResult
  }


  private def processPipeline(dispatcher: TaskDispatcher, pipeline: Pipeline, options: SchedulerOptions, jar: String, dispatchedPipelines: Counter, failedPipelines: Counter): Unit = {
    val timerInst = scheduleTimer.time()
    logger.info(s"Dispatching pipleine ${pipeline.name}")

    dispatcher.dispatch(pipeline, options, jar, conf) match {
      case Left(ex) =>
        ErrorHandler.pipelineScheduleFailed(pipeline, ex.getMessage())
        logger.warn(
          s"failed to deploy pipeline ${pipeline.name} in ${TimeUnit.SECONDS.convert(timerInst.stop(), TimeUnit.NANOSECONDS)}"
        )
        failedPipelines.inc()
      case Right(r) =>
        logger.info(
          s"dispatched pipeline ${pipeline.name} in ${TimeUnit.SECONDS.convert(timerInst.stop(), TimeUnit.NANOSECONDS)}"
        )
        dispatchedPipelines.inc()
        // activation successful - delete the failure counter
        db.run(ScheduleFailureCounters().filter(_.pipelineId === pipeline.id.get).delete).waitForResult
    }

    // update the next run time for this pipeline
    updateNextRunTime(pipeline, options)
  }

  def run(options: SchedulerOptions): Unit = {

    logger.info(s"run with options: $options")

    val actualStart = options.actualStart
    db.run(DBIO.seq(SchedulerMetrics() += SchedulerMetric(actualStart))).waitForResult

    // in case of default taskDispatcher: the dispatchedPipelines and succesfulPipelines metrics should be exactly same.
    val dispatchedPipelines = metrics.register("counter.successful-pipeline-dispatch-count", new Counter())
    val successfulPipelines = metrics.register("counter.successful-pipeline-deployment-count", new Counter())
    val failedPipelines = metrics.register("counter.failed-pipeline-deployment-count", new Counter())

    val taskDispatcher: TaskDispatcher = conf.dispatcherType match {
      case "default" => new TaskDispatcherImpl()
      case x =>
        // pipelines scheduled in a previous run should be fetched here if the dispatcher is remote
        throw new NotImplementedError(s"there is no task dispatcher implementation for $x")
    }

    // fetch the pipelines which may have been scheduled in a previous runs but are not present in the database yet
    // this operation is a no-op in case of a local dispatcher like the TaskDispatcherImpl
    val previouslyScheduledPipelines = taskDispatcher.retrieve(conf)
    successfulPipelines.inc(previouslyScheduledPipelines.length)
    updateScheduledPipelines(previouslyScheduledPipelines)

    val pipelineModels = pendingPipelineRecords(options.scheduledEnd)
    db.run(DBIO.seq(
        SchedulerMetrics()
          .filter(_.startTime === actualStart)
          .map(_.pipelineCount)
          .update(Option(pipelineModels.size))
      ))
      .waitForResult
    metrics.register("gauges.pipeline_count", new ConstantValueGauge(pipelineModels.size))

    // TODO: this variable can be moved to the implementation
    lazy val localJars = getLocalJars(pipelineModels)

    // execute all jars
    val parPipelineModels = pipelineModels.par

    if (parallel > 0)
      parPipelineModels.tasksupport = new ForkJoinTaskSupport(
        new ForkJoinPool(parallel * Runtime.getRuntime.availableProcessors)
      )

    parPipelineModels.foreach(p => processPipeline(taskDispatcher, p, options, localJars(p.jar), dispatchedPipelines, failedPipelines))

    // retrieve the scheduled pipelines and save the information in the db
    val scheduledPipelines = taskDispatcher.retrieve(conf)
    successfulPipelines.inc(scheduledPipelines.length)
    updateScheduledPipelines(scheduledPipelines)

    db.run(DBIO.seq(
        SchedulerMetrics()
          .filter(_.startTime === actualStart)
          .map(_.endTime)
          .update(Option(DateTime.now))
      ))
      .waitForResult
  }

  /**
   * @param args Extra envs e.g. (ENV1=x ENV2=y ...)
   */
  def main(args: Array[String]): Unit = {

    val start = System.nanoTime
    val mainTimer = new SimpleTimerGauge(TimeUnit.MINUTES)
    metrics.register("gauges.runtime", mainTimer)

    val reporter = reportingEngine.getReporter(metrics)

    try {
      SchedulerOptionParser.parse(args) match {
        case Some(options) => run(options)
        case None => ErrorExit.invalidCommandlineArguments(logger)
      }

      val timeSpan = (System.nanoTime - start) / 1E9
      logger.info(s"All pipelines scheduled in $timeSpan seconds")
    } finally {
      mainTimer.stop()
      reporter.report()
      reporter.close()
    }
  }

}
