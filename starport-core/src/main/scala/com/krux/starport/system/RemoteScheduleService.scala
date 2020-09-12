package com.krux.starport.system

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.sqs.model.{ReceiveMessageRequest, SendMessageRequest}
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.krux.hyperion.expression.{Duration => HDuration}
import com.krux.starport.Logging
import com.krux.starport.cli.SchedulerOptions
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.WaitForIt.WaitForAwaitable
import com.krux.starport.db.record.{Pipeline, ScheduledPipeline}
import com.krux.starport.db.table.{Pipelines, ScheduleFailureCounters, ScheduledPipelines}
import com.krux.starport.util.DateTimeFunctions.{timesTillEnd, _}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import slick.jdbc.PostgresProfile.api._

import scala.collection.JavaConverters._

object RemoteScheduleService extends Logging {

  val sqsMessageVisibilityTimeoutInSec = 30

  def schedule(
    options: SchedulerOptions,
    pipelines: List[Pipeline],
    starportSetting: StarportSettings
  ): Either[Exception, Int] = {

    syncRemotePipelineStatus(starportSetting)
    sendRemotePipelineRequest(pipelines, options, starportSetting)
  }

  /** Get remote pipeline status from SQS and update DB */
  def syncRemotePipelineStatus(starportSetting: StarportSettings): Unit = {
    val client = getAwsClient(starportSetting)

    def processSqsMessages(): Int = {
      val receiveMessageRequest = new ReceiveMessageRequest(starportSetting.remoteSchedulerReturnAddress).withVisibilityTimeout(sqsMessageVisibilityTimeoutInSec)
      val messageResult = client.receiveMessage(receiveMessageRequest)
      logger.info(s"Number of remote pipeline complete messages are ${messageResult.getMessages.size()}")

      val messages = messageResult.getMessages.asScala

      // For each pipeline "delete schedule failure counter", "update next runtime" and "insert scheduled pipeline record"
      messages.foreach(m => {
        val db = starportSetting.jdbc.db
        val remotePipelineResponsesEither = decode[Seq[RemotePipelineResponse]](m.getBody)

        remotePipelineResponsesEither match {
          case Left(e) =>
            logger.error(s"Invalid remote pipeline done response json ${m.getBody}", e)

          case Right(remotePipelineResponses) =>
            val scheduledPipelines = remotePipelineResponses.map(r =>
              ScheduledPipeline(
                r.awsId,
                r.pipelineId,
                r.pipelineName,
                r.options.scheduledStart.toLocalDateTime,
                r.options.actualStart.toLocalDateTime,
                r.deployedTime.toLocalDateTime,
                r.status, true))

            val pipelineIds = scheduledPipelines.map(_.pipelineId).distinct //ideally only one pipeline id per message
            logger.info(s"Remote scheduled pipelineIds $pipelineIds awsIds: ${scheduledPipelines.map(_.awsId)}")

            pipelineIds.foreach(pipelineId => {
              val pipelineOption = db.run(Pipelines().filter(_.id === pipelineId).result.headOption).waitForResult
              if (pipelineOption.isEmpty) logger.error(s"Pipeline id $pipelineId returned by remote scheduler not found in db")

              pipelineOption.foreach(pipeline => {
                db.run(ScheduleFailureCounters().filter(_.pipelineId === pipeline.id).delete).waitForResult
                updateNextRunTime(pipeline, remotePipelineResponses.head.options.scheduledEnd.toLocalDateTime, starportSetting)
              })
            })

            val insertAction = DBIO.seq(ScheduledPipelines() ++= scheduledPipelines)
            db.run(insertAction).waitForResult
        }

        val deleteMessageResult = client.deleteMessage(starportSetting.remoteSchedulerReturnAddress, m.getReceiptHandle)
        logger.debug(s"Remote complete SQS messageId ${m.getMessageId} deleted $deleteMessageResult")
      })

      messages.size
    }

    processSqsMessages() match {
      case 0 => logger.info(s"Remote pipeline sync completed")
      case _ => processSqsMessages()
    }
  }

  private def updateNextRunTime(pipelineRecord: Pipeline, scheduledEnd: LocalDateTime, starportSetting: StarportSettings): Int = {
    // update the next runtime in the database
    val newNextRunTime = nextRunTime(pipelineRecord.nextRunTime.get, HDuration(pipelineRecord.period), scheduledEnd)
    val updateQuery = Pipelines().filter(_.id === pipelineRecord.id).map(_.nextRunTime)
    logger.debug(s"Pipeline nextRunTime update query ${updateQuery.updateStatement}")
    val updateAction = updateQuery.update(Some(newNextRunTime))
    starportSetting.jdbc.db.run(updateAction).waitForResult
  }

  def sendRemotePipelineRequest(pipelines: List[Pipeline], options: SchedulerOptions, starportSetting: StarportSettings): Either[Exception, Int] = {
    try {
      val client = getAwsClient(starportSetting)

      val messageIds = pipelines.map( p => {
        val messageBodyJson = getRemotePipelineRequestJson(p, options, starportSetting)
        val sendMessageRequest = new SendMessageRequest(starportSetting.remoteSchedulerToAddress, messageBodyJson)
        val sendMessageResult = client.sendMessage(sendMessageRequest)
        val messageId = sendMessageResult.getMessageId

        logger.info(s"Remote pipeline request submitted for ${p.id} with messageId $messageId and messageBody $messageBodyJson")
      })

      Right(messageIds.size)
    } catch {
      case e: Exception =>
        logger.error(s"Exception in sending remote pipeline requests ${pipelines.flatMap(_.id)}", e)
        Left(e)
    }
  }

  def getRemotePipelineRequestJson(pipeline: Pipeline, options: SchedulerOptions, starportSetting: StarportSettings): String = {
    RemotePipelineRequest(
      pipeline.id.get,
      pipelineName = s"${starportSetting.pipelinePrefix}${options.actualStart}_${pipeline.id.getOrElse(0)}_${pipeline.`class`}",
      pipeline.jar,
      pipeline.`class`,
      getUtcTime(pipeline.nextRunTime.get),
      times = getPipelineNumRunTimes(pipeline, options),
      pipeline.period,
      SchedulerOptionsWithTimeZone(getUtcTime(options.scheduledStart), getUtcTime(options.scheduledEnd), getUtcTime(options.actualStart)),
      starportSetting.remoteSchedulerReturnAddress
    ).asJson.noSpaces
  }

  def getPipelineNumRunTimes(pipeline: Pipeline, options: SchedulerOptions): Int = {
    val start = pipeline.nextRunTime.get
    val until = pipeline.end
      .map(LocalDateTimeOrdering.min(options.scheduledEnd, _))
      .getOrElse(options.scheduledEnd)
    val pipelinePeriod = pipeline.period

    // Note that aws datapieline have a weird requirement for endTime (documented as it has to be
    // greater than startTime, but actually it has to be greater than startTime + period), it's
    // very confusing, we here change it to number of times the pipeline should run to avoid this
    // confusion.
    val calculatedTimes =
    if (pipeline.backfill) timesTillEnd(start, until, HDuration(pipelinePeriod))
    else 1

    if (calculatedTimes < 1) {
      // the calculatedTimes should never be < 1
      logger.error(s"calculatedTimes < 1")
    }
    Math.max(1, calculatedTimes)
  }

  def getAwsClient(starportSetting: StarportSettings): AmazonSQS = {
    starportSetting.awsEndpointUrl match {
      case Some(awsEndpointUrl) =>
        //localhost so any region should work
        val endpoint = new AwsClientBuilder.EndpointConfiguration(awsEndpointUrl, "us-west-2")
        AmazonSQSClientBuilder.standard.withEndpointConfiguration(endpoint).build

      case None => AmazonSQSClientBuilder.defaultClient()
    }
  }

  def getUtcTime(localDateTime: LocalDateTime): ZonedDateTime = localDateTime.atZone(ZoneId.of("UTC"))
  
  case class RemotePipelineRequest(
    pipelineId: Int,
    pipelineName: String,
    jarS3Location: String,
    mainClass: String,
    startTime: ZonedDateTime,
    times: Int,
    period: String,
    options: SchedulerOptionsWithTimeZone,
    returnAddress: String
  )

  case class RemotePipelineResponse(
    pipelineId: Int,
    awsId: String,
    pipelineName: String,
    deployedTime: ZonedDateTime,
    status: String,
    options: SchedulerOptionsWithTimeZone
  )

  case class SchedulerOptionsWithTimeZone(
    scheduledStart: ZonedDateTime,
    scheduledEnd: ZonedDateTime,
    actualStart: ZonedDateTime
  )
}
