package com.krux.starport.db.table

import java.time.LocalDateTime

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.db.record.FailedPipeline


class FailedPipelines(tag: Tag)
  extends Table[FailedPipeline](tag, "failed_pipelines") {

  def awsId = column[String]("aws_id", O.PrimaryKey, O.SqlType("VARCHAR(254)"))

  def pipelineId = column[Int]("pipeline_id")

  def resolved = column[Boolean]("resolved")

  def checkedTime = column[LocalDateTime]("checked_time")

  def * = (awsId, pipelineId, resolved, checkedTime) <> (FailedPipeline.tupled, FailedPipeline.unapply)

  def pipeline = foreignKey("failed_pipelines_pipelines_fk", pipelineId, TableQuery[Pipelines])(
    _.id, onUpdate = ForeignKeyAction.Restrict, onDelete = ForeignKeyAction.Cascade)

}

object FailedPipelines {
  def apply() = TableQuery[FailedPipelines]
}
