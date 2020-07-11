package com.krux.starport.db.table

import java.time.LocalDateTime

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.db.record.ScheduledPipeline


class ScheduledPipelines(tag: Tag) extends Table[ScheduledPipeline](tag, "scheduled_pipelines") {

  /**
   * The pipeline id that are returned from aws
   */
  def awsId = column[String]("aws_id", O.PrimaryKey, O.SqlType("VARCHAR(254)"))

  /**
   * The corresponding pipeline id
   */
  def pipelineId = column[Int]("pipeline_id")

  /**
   * The pipline name that is used
   */
  def pipelineName = column[String]("pipeline_name", O.SqlType("VARCHAR(500)"))

  /**
   * The scheduled start time of the scheduler
   */
  def scheduledStart = column[LocalDateTime]("scheduled_start")

  /**
   * The actual start time of the scheduler
   */
  def actualStart = column[LocalDateTime]("actual_start")

  /**
   * When the pipeline is deployed
   */
  def deployedTime = column[LocalDateTime]("deployed_time")

  /**
   * The status that are returned by aws
   */
  def status = column[String]("status", O.SqlType("VARCHAR(40)"))

  /**
   * If this still shows in the AWS Data Pipeline console
   */
  def inConsole = column[Boolean]("in_console")

  def * = (awsId, pipelineId, pipelineName, scheduledStart, actualStart, deployedTime, status, inConsole) <>
    (ScheduledPipeline.tupled, ScheduledPipeline.unapply)

  def pipeline = foreignKey("sheduled_pipelines_pipelines_fk", pipelineId, TableQuery[Pipelines])(
    _.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

}

object ScheduledPipelines {
  def apply() = TableQuery[ScheduledPipelines]
}
