package com.krux.starport.util

import scala.concurrent.{ExecutionContext, Future}

import org.joda.time.DateTime
import slick.jdbc.PostgresProfile.api._

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.{Pipeline, ScheduleFailureCounter}
import com.krux.starport.db.table.Pipelines
import com.krux.starport.db.table.ScheduleFailureCounters
import com.krux.starport.db.WaitForIt
import com.krux.starport.Logging
import com.krux.starport.util.notification.SendEmail


/**
 * Handles database related logics for handling piplines
 */
object ErrorHandler extends Logging with WaitForIt {

  // TODO Make this configurable
  final val MaxSchedulingFailure = 3

  /**
   * Tracks how many scheduling failure occurs consecutively and deactivates the pipeline if it
   * exceeds a threshold (i.e. 3).
   *
   * @return the SES send ID
   */
  def pipelineScheduleFailed(pipeline: Pipeline, errorMessage: String)
    (implicit conf: StarportSettings, ec: ExecutionContext): String = {

    val db = conf.jdbc.db

    val pipelineId = pipeline.id.get
    val fromEmail = conf.fromEmail
    //TODO add pipeline.owner.getOrElse(PagerDutyEmail)
    val toEmails = conf.toEmails

    def handlePipelineAndNotify(failureCount: Int): Future[String] = {
      if (failureCount >= MaxSchedulingFailure) {  // deactivate the pipeline if it reaches max # of failures
        val deactivatePipelineQuery =
          Pipelines().filter(_.id === pipelineId).map(_.isActive).update(false)

        db.run(deactivatePipelineQuery).map { _ =>
          SendEmail(
            toEmails,
            fromEmail,
            s"[ACTION NEEDED] Pipline ${pipeline.name} has been deactivated due to scheduling failure",
            errorMessage
          )
        }
      } else {  // increment the count, and send the notification
        val newCount = failureCount + 1
        val setScheduleFailureCountQuery = ScheduleFailureCounters()
          .insertOrUpdate(ScheduleFailureCounter(pipelineId, newCount, DateTime.now))

        // Set schedule failure count
        db.run(setScheduleFailureCountQuery).map { _ =>
          SendEmail(
            toEmails,
            fromEmail,
            s"[ACTION NEEDED] Pipline ${pipeline.name} failed to schedule ($newCount/$MaxSchedulingFailure)",
            s"It will be deactivated after the number of schedule failures reach $MaxSchedulingFailure\n\n$errorMessage"
          )
        }
      }
    }

    val result: Future[String] = for {
      failureCounts <- db.run(ScheduleFailureCounters().filter(c => c.pipelineId === pipelineId).take(1).result)
      failureCount = failureCounts.headOption.map(_.failureCount).getOrElse(0)
      notifyReqId <- handlePipelineAndNotify(failureCount)
    } yield notifyReqId

    result.waitForResult

  }

  /**
   * @return the SES send ID
   */
  def cleanupActivityFailed(pipeline: Pipeline, stackTrace: Array[StackTraceElement])
    (implicit conf: StarportSettings): String = {
    SendEmail(
      conf.toEmails,
      conf.fromEmail,
      s"[Starport Cleanup Failure] cleanup activity failed for ${pipeline.name}",
      stackTrace.mkString("\n")
    )
  }

}
