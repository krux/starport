package com.krux.starport.dispatcher.impl

import java.time.LocalDateTime
import java.util.concurrent.ConcurrentLinkedQueue

import scala.sys.process.{Process, ProcessLogger}
import scala.util.{Either, Left, Right}

import com.krux.hyperion.client.{AwsClient, AwsClientForName}
import com.krux.hyperion.expression.{Duration => HDuration}
import com.krux.starport.cli.SchedulerOptions
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.Pipeline
import com.krux.starport.db.record.ScheduledPipeline
import com.krux.starport.dispatcher.impl.ConcurrentQueueHelpers._
import com.krux.starport.dispatcher.TaskDispatcher
import com.krux.starport.exception.StarportException
import com.krux.starport.Logging
import com.krux.starport.util.DateTimeFunctions


class TaskDispatcherImpl extends TaskDispatcher with DateTimeFunctions with Logging {

  override def dispatch(pipeline: Pipeline, options: SchedulerOptions, jar: String, conf: StarportSettings) = {
    val result = for {
      pipelineName <- deployPipeline(pipeline, options.scheduledStart, options.scheduledEnd, jar, conf)
      scheduledPipelines <- activatePipeline(pipeline, pipelineName, options, conf)
    } yield {
      scheduledPipelines
    }

    // keep the scheduled pipelines in a queue and return Either[Exception, Boolean]
    result map { (scheduledPipelines) =>
      scheduledPipelines.foreach(activatedPipelines.add(_))
      ()
    }
  }

  override def retrieve(conf: StarportSettings) = {
    activatedPipelines.retrieveAll()
  }

  private val activatedPipelines =  new ConcurrentLinkedQueue[ScheduledPipeline]()

  private def activatePipeline(
      pipelineRecord: Pipeline,
    pipelineName: String,
    options: SchedulerOptions,
    conf: StarportSettings
  ): Either[StarportException, Seq[ScheduledPipeline]] = {
    val logPrefix = s"|PipelineId: ${pipelineRecord.id}|"

    logger.info(s"$logPrefix Activating pipeline: $pipelineName...")

    val awsClientForName = AwsClientForName(AwsClient.getClient(), pipelineName, conf.maxRetry)
    val pipelineIdNameMap = awsClientForName.pipelineIdNames

    awsClientForName
      .forId() match {
        case Some(client) =>
          val activationStatus = client.activatePipelines() match {
            case Some(clientForId) => "success"
            case _ => "fail"
          }

         Right(client.pipelineIds.toSeq.map(awsId =>
           ScheduledPipeline(
             awsId,
             pipelineRecord.id.get,
             pipelineIdNameMap(awsId),
             options.scheduledStart,
             options.actualStart,
             currentTimeUTC().toLocalDateTime(),
             activationStatus,
             true
           )))
        case None =>
            Left(new StarportException(s"pipeline with name $pipelineName not found"))
      }
  }


  /**
   * @return status, the output, and the deployed pipeline name
   */
  private def deployPipeline(
      pipelineRecord: Pipeline,
      currentTime: LocalDateTime,
      currentEndTime: LocalDateTime,
      localJar: String,
      conf: StarportSettings
    ): Either[StarportException, String] = {
    val logPrefix = s"|PipelineId: ${pipelineRecord.id}|"

    logger.info(s"$logPrefix Deploying pipeline: ${pipelineRecord.name}")

    val start = pipelineRecord.nextRunTime.get
    val until = pipelineRecord.end
      .map(LocalDateTimeOrdering.min(currentEndTime, _))
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

    val actualStart = currentTimeUTC().format(DateTimeFormat)

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
      "--start", start.format(DateTimeFormat),
      "--times", times.toString,
      "--every", pipelinePeriod,
      "--name", pipelineName
    ) ++ conf.region.toSeq.flatMap(r => Seq("--region", r.getName))

    val process = Process(
      command,
      None,
      conf.extraEnvs.toSeq: _*
    )

    logger.info(s"$logPrefix Executing `${command.mkString(" ")}`")

    val outputBuilder = new StringBuilder
    val status = process ! ProcessLogger(line => outputBuilder.append(line + "\n"))

    status match {
      case 0 => Right(pipelineName)
      case _ => Left(new StarportException(s"status: $status, logs : ${outputBuilder.toString}"))
    }

  }

}
