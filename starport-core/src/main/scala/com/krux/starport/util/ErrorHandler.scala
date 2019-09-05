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
import com.krux.starport.util.notification.Notify


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
  def pipelineScheduleFailed(pipeline: Pipeline, allOutput: String)
    (implicit conf: StarportSettings, ec: ExecutionContext): String = {

    val db = conf.jdbc.db

    val pipelineId = pipeline.id.get

    def handlePipelineAndNotify(failureCount: Int): Future[String] = {
      if (failureCount >= MaxSchedulingFailure) {  // deactivate the pipeline if it reaches max # of failures
        val deactivatePipelineQuery =
          Pipelines().filter(_.id === pipelineId).map(_.isActive).update(false)

        db.run(deactivatePipelineQuery).map { _ =>
          Notify(
            s"[ACTION NEEDED] Pipeline ${pipeline.name} has been deactivated due to scheduling failure",
            allOutput,
            pipeline
          )
        }
      } else {  // increment the count, and send the notification
        val newCount = failureCount + 1
        val setScheduleFailureCountQuery = ScheduleFailureCounters()
          .insertOrUpdate(ScheduleFailureCounter(pipelineId, newCount, DateTime.now))

        // Set schedule failure count
        db.run(setScheduleFailureCountQuery).map { _ =>
          Notify(
            s"[ACTION NEEDED] Pipeline ${pipeline.name} failed to schedule ($newCount/$MaxSchedulingFailure)",
            s"It will be deactivated after the number of schedule failures reach $MaxSchedulingFailure\n\n$allOutput",
            pipeline
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
    val stackTraceMessage = stackTrace.mkString("\n")
    logger.warn(s"cleanup activity failed for pipeline ${pipeline.id}, because: $stackTraceMessage")
    Notify(
      s"[Starport Cleanup Failure] cleanup activity failed for ${pipeline.name}",
      stackTraceMessage,
      pipeline
    )
  }

}
