package com.krux.starport.system

import java.time.LocalDateTime

import akka.actor.typed.scaladsl._
import akka.actor.typed.{ActorRef, Behavior}
import com.codahale.metrics.Timer
import slick.jdbc.PostgresProfile.api._

import com.krux.hyperion.expression.{Duration => HDuration}
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.{Pipeline, ScheduledPipeline}
import com.krux.starport.db.table.ScheduledPipelines
import com.krux.starport.db.table.{Pipelines, ScheduleFailureCounters}
import com.krux.starport.db.WaitForIt
import com.krux.starport.exception.StarportException
import com.krux.starport.util.DateTimeFunctions._
import com.krux.starport.util.{DataPipelineDeploy, ErrorHandler}

object DispatcherActor {

  trait Msg

  case class ScheduleReq(
    pipeline: Pipeline,
    scheduledStart: LocalDateTime,
    scheduledEnd: LocalDateTime,
    actualStart: LocalDateTime,
    localJar: String,
    timerContext: Timer.Context,
    replyTo: ActorRef[ScheduleRsp]
  ) extends Msg

  case class ScheduleRsp(
    scheduledPipelines: Either[StarportException, Seq[ScheduledPipeline]],
    dispatcherAct: ActorRef[Msg]
  )

  def apply(starportSetting: StarportSettings): Behavior[Msg] =
    Behaviors.setup(new DispatcherActor(_, starportSetting))

}

class DispatcherActor(context: ActorContext[DispatcherActor.Msg], starportSetting: StarportSettings)
    extends AbstractBehavior[DispatcherActor.Msg](context) with WaitForIt {

  import DispatcherActor._

  override def onMessage(msg: Msg): Behavior[Msg] = msg match {
    case ScheduleReq(
          pipeline,
          scheduledStart,
          scheduledEnd,
          actualStart,
          localJar,
          timerContext,
          replyTo
        ) =>
      context.log.info(s"${context.self} dispatching pipline ${pipeline.name} (${pipeline.id})")
      val result = DataPipelineDeploy.deployAndActive(
        pipeline,
        scheduledStart,
        scheduledEnd,
        actualStart,
        localJar,
        starportSetting,
        context.log
      )

      // TODO it would be more moduler if this is done in a separate actor
      result match {
        case Left(ex) =>
          context.log.warn(s"failed to deploy pipeline ${pipeline.name}, handling error...")
          ErrorHandler.pipelineScheduleFailed(pipeline, ex.getMessage)(
            starportSetting,
            context.executionContext
          )
        case Right(rs) =>
          context.log
            .info(s"successfuly deployed pipeline ${pipeline.name}, reset error counter in DB")
          // activation successful - delete the failure counter
          starportSetting.jdbc.db
            .run(ScheduleFailureCounters().filter(_.pipelineId === pipeline.id.get).delete)
            .waitForResult
          updateNextRunTime(pipeline, scheduledEnd)

          val insertAction = DBIO.seq(ScheduledPipelines() ++= rs)
          starportSetting.jdbc.db.run(insertAction).waitForResult
      }
      timerContext.stop()

      replyTo ! ScheduleRsp(result, context.self)
      this
  }

  private def updateNextRunTime(pipelineRecord: Pipeline, scheduledEnd: LocalDateTime) = {
    // update the next runtime in the database
    val newNextRunTime =
      nextRunTime(pipelineRecord.nextRunTime.get, HDuration(pipelineRecord.period), scheduledEnd)
    val updateQuery = Pipelines().filter(_.id === pipelineRecord.id).map(_.nextRunTime)
    context.log.debug(s"Update with query ${updateQuery.updateStatement}")
    val updateAction = updateQuery.update(Some(newNextRunTime))
    starportSetting.jdbc.db.run(updateAction).waitForResult
  }

}
