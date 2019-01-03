package com.krux.starport

import java.util.concurrent.{ForkJoinPool, TimeUnit}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.ExecutionContext.Implicits.global
import scala.sys.process._

import com.codahale.metrics.MetricRegistry
import com.github.nscala_time.time.Imports._
import slick.jdbc.PostgresProfile.api._

import com.krux.hyperion.client.{AwsClientForName, AwsClient}
import com.krux.hyperion.expression.{Duration => HDuration}
import com.krux.starport.cli.{SchedulerOptions, SchedulerOptionParser}
import com.krux.starport.db.record.{Pipeline, ScheduledPipeline, SchedulerMetric}
import com.krux.starport.db.table.{ScheduledPipelines, Pipelines, SchedulerMetrics, ScheduleFailureCounters}
import com.krux.starport.metric.{ConstantValueGauge, SimpleTimerGauge, MetricSettings}
import com.krux.starport.util.{S3FileHandler, ErrorHandler}


object StartScheduledPipelines extends StarportActivity {

  val metrics = new MetricRegistry()

  val scheduleTimer = metrics.timer("timers.pipeline_scheduling_time")

  val taskName = "StartScheduledPipelines"

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

  /**
   * @return status, the output, and the deployed pipeline name
   */
  def deployPipeline(
      pipelineRecord: Pipeline,
      currentTime: DateTime,
      currentEndTime: DateTime,
      localJar: String
    ): (Int, String, String) = {

    // TODO probably better to just use a different logger
    val logPrefix = s"|PipelineId: ${pipelineRecord.id}|"

    logger.info(s"$logPrefix Deploying pipeline: ${pipelineRecord.name}")

    val start = pipelineRecord.nextRunTime.get
    val until = pipelineRecord.end
      .map(DateTimeOrdering.min(currentEndTime, _))
      .getOrElse(currentEndTime)
    val pipelinePeriod = pipelineRecord.period

    // Note that aws datapieline have a weird requirement for endTime (documented as it has to be
    // greater than startTime, but actually it has to be greater than startTime + period), it's
    // very confusing, we here change it to number of times the pipeline should run to avoid this
    // confusion.
    val calculatedTimes =
      if (pipelineRecord.backfill) timesTillEnd(start, until, HDuration(pipelinePeriod))
      else 1

    logger.info(s"$logPrefix calculatedTimes: $calculatedTimes")
    if (calculatedTimes < 1) {
      // the calculatedTimes should never be < 1
      logger.error(s"calculatedTimes < 1")
    }
    val times = Math.max(1, calculatedTimes)

    val actualStart = DateTime.now.withZone(DateTimeZone.UTC).toString(DateTimeFormat)

    val pipelineClass = pipelineRecord.`class`
    val pipelineName = s"${conf.pipelinePrefix}${actualStart}_${pipelineRecord.id.getOrElse(0)}_${pipelineClass}"

    // create the pipeline through cli but do not activiate it
    val command = Seq(
      "java",
      "-cp",
      localJar,
      pipelineClass,
      "create",
      "--no-check",
      "--start", start.toString(DateTimeFormat),
      "--times", times.toString,
      "--every", pipelinePeriod,
      "--name", pipelineName
    )

    val process = Process(
      command,
      None,
      extraEnvs: _*
    )

    logger.info(s"$logPrefix Executing `${command.mkString(" ")}`")

    val outputBuilder = new StringBuilder
    val status = process ! ProcessLogger(line => outputBuilder.append(line + "\n"))

    (status, outputBuilder.toString, pipelineName)

  }

  def activatePipeline(
      pipelineRecord: Pipeline,
      pipelineName: String,
      scheduledStart: DateTime,
      actualStart: DateTime,
      scheduledEnd: DateTime
    ) = {

    // TODO probably better to just use a different logger
    val logPrefix = s"|PipelineId: ${pipelineRecord.id}|"

    logger.info(s"$logPrefix Activating pipeline: $pipelineName...")

    val awsClientForName = AwsClientForName(AwsClient.getClient(), pipelineName, conf.maxRetry)
    val pipelineIdNameMap = awsClientForName.pipelineIdNames

    awsClientForName
      .forId() match {
        case Some(client) =>
          val activationStatus = if (client.activatePipelines().nonEmpty) {
            "success"
          } else {
            logger.error(s"$logPrefix Failed to activate pipeline ${client.pipelineIds}")
            "fail"
          }

          logger.info(s"$logPrefix Register pipelines (${client.pipelineIds}) in database.")

          val scheduledPipelineRecords = client.pipelineIds.map(awsId =>
            ScheduledPipeline(
              awsId,
              pipelineRecord.id.get,
              pipelineIdNameMap(awsId),
              scheduledStart,
              actualStart,
              DateTime.now,
              activationStatus,
              true
            )
          )

          val insertAction = DBIO.seq(ScheduledPipelines() ++= scheduledPipelineRecords)
          db.run(insertAction).waitForResult

          logger.info(s"$logPrefix updating the next run time")

          // update the next runtime in the database
          val newNextRunTime = nextRunTime(pipelineRecord.nextRunTime.get, HDuration(pipelineRecord.period), scheduledEnd)
          val updateQuery = Pipelines().filter(_.id === pipelineRecord.id).map(_.nextRunTime)
          logger.debug(s"$logPrefix Update with query ${updateQuery.updateStatement}")
          val updateAction = updateQuery.update(Some(newNextRunTime))
          db.run(updateAction).waitForResult

          // activate successful, reset the failure counter, by deleting it
          db.run(ScheduleFailureCounters().filter(_.pipelineId === pipelineRecord.id.get).delete).waitForResult

          logger.info(s"$logPrefix Successfully scheduled pipeline $pipelineName")
        case None =>
          val errorMessage = s"pipeline with name $pipelineName not found"
          logger.error(errorMessage)
          ErrorHandler.pipelineScheduleFailed(pipelineRecord, errorMessage)
      }
  }

  def run(options: SchedulerOptions): Unit = {

    logger.info(s"run with options: $options")

    val actualStart = options.actualStart
    db.run(DBIO.seq(SchedulerMetrics() += SchedulerMetric(actualStart))).waitForResult

    val pipelineModels = pendingPipelineRecords(options.scheduledEnd)
    db.run(DBIO.seq(
        SchedulerMetrics()
          .filter(_.startTime === actualStart)
          .map(_.pipelineCount)
          .update(Option(pipelineModels.size))
      ))
      .waitForResult
    metrics.register("gauges.pipeline_count", new ConstantValueGauge(pipelineModels.size))

    val localJars = getLocalJars(pipelineModels)

    // execute all jars
    val parPipelineModels = pipelineModels.par

    if (parallel > 0)
      parPipelineModels.tasksupport = new ForkJoinTaskSupport(
        new ForkJoinPool(parallel * Runtime.getRuntime.availableProcessors)
      )

    parPipelineModels.foreach { p =>

      val timerInst = scheduleTimer.time()

      logger.info(s"deploying pipleine ${p.name}")

      val (status, output, pipelineName) = deployPipeline(
        p, options.scheduledStart, options.scheduledEnd, localJars(p.jar))

      if (status == 0) {  // deploy successfully, perform activation
        activatePipeline(p, pipelineName, options.scheduledStart, options.actualStart, options.scheduledEnd)
      } else {  // otherwise handle the failure and send notification
        ErrorHandler.pipelineScheduleFailed(p, output)
      }

      val nano = timerInst.stop()
      logger.info(s"deployed pipeline ${p.name} in ${TimeUnit.SECONDS.convert(nano, TimeUnit.NANOSECONDS)}")
    }

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

    val reporter = MetricSettings.getReporter(conf.metricSettings, metrics)

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
    }
  }

}
