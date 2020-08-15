package com.krux.starport.system

import akka.actor.typed.scaladsl._
import akka.actor.typed.{ActorRef, Behavior}
import akka.pattern.StatusReply
import com.codahale.metrics.Timer

import com.krux.starport.cli.SchedulerOptions
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.Pipeline

object SchedulerActor {

  trait Msg
  case object Completed extends Msg
  case class WrappedDispatcherResp(resp: DispatcherActor.ScheduleRsp) extends Msg

  def apply(
    options: SchedulerOptions,
    localJars: Map[String, String],
    numDispatchers: Int,
    pipelines: List[Pipeline],
    starportSetting: StarportSettings,
    scheduleTimer: Timer,
    replyTo: ActorRef[StatusReply[Int]]
  ) = Behaviors
    .setup(context =>
      new SchedulerActor(
        context,
        options,
        localJars,
        numDispatchers,
        pipelines,
        starportSetting,
        scheduleTimer,
        replyTo
      )
    )

}

class SchedulerActor(
  context: ActorContext[SchedulerActor.Msg],
  options: SchedulerOptions,
  localJars: Map[String, String],
  numDispatchers: Int,
  pipelines: List[Pipeline],
  starportSetting: StarportSettings,
  scheduleTimer: Timer,
  replyTo: ActorRef[StatusReply[Int]]
) extends AbstractBehavior[SchedulerActor.Msg](context) {

  import SchedulerActor._

  val totalPipelines = pipelines.size

  val dispatcherActors = (0 until numDispatchers)
    .map(i => context.spawn(DispatcherActor(starportSetting), s"pipeline-dispatcher-$i"))

  val dispatcherRespAdaptor = context.messageAdapter(WrappedDispatcherResp.apply)

  // Actor states
  var pipelinesToBeScheduled: List[Pipeline] = pipelines
  var completedPipelineCount = 0

  // Start Scheduling when this actor is created
  onStart()

  def onStart(): Unit =
    if (totalPipelines == 0) {
      context.self ! Completed
    } else {
      val (thisBatch, nextBatch) = pipelinesToBeScheduled.splitAt(numDispatchers)
      pipelinesToBeScheduled = nextBatch
      thisBatch.zip(dispatcherActors).foreach {
        case (pipeline, dispatcherActor) =>
          dispatcherActor ! DispatcherActor.ScheduleReq(
            pipeline,
            options.scheduledStart,
            options.scheduledEnd,
            options.actualStart,
            localJars(pipeline.jar),
            scheduleTimer.time(),
            dispatcherRespAdaptor
          )
      }
    }

  override def onMessage(msg: Msg): Behavior[Msg] = msg match {

    case WrappedDispatcherResp(resp) =>
      completedPipelineCount += 1
      pipelinesToBeScheduled match {
        case next :: rest =>
          // send the next pipeline to this actor
          resp.dispatcherAct ! DispatcherActor.ScheduleReq(
            next,
            options.scheduledStart,
            options.scheduledEnd,
            options.actualStart,
            localJars(next.jar),
            scheduleTimer.time(),
            dispatcherRespAdaptor
          )
          pipelinesToBeScheduled = rest
          this
        case Nil if completedPipelineCount < totalPipelines =>
          // do nothing if nothing left to schedule but have not received all responses het
          this
        case _ =>
          context.self ! Completed
          this
      }

    case Completed =>
      replyTo ! StatusReply.Success(completedPipelineCount)
      Behaviors.stopped

  }

}
