package com.krux.starport.db.table

import org.joda.time.DateTime
import slick.jdbc.PostgresProfile.api._

import com.krux.starport.db.DateTimeMapped
import com.krux.starport.db.record.PipelineHistory


class PipelineHistories(tag: Tag) extends Table[PipelineHistory](tag, "pipeline_histories") with DateTimeMapped {

  def pipelineId = column[Int]("pipeline_id")

  def nextRunTime = column[Option[DateTime]]("next_run_time")

  def status = column[String]("status", O.SqlType("VARCHAR(40)"))

  def * = (pipelineId, nextRunTime, status) <> (PipelineHistory.tupled, PipelineHistory.unapply)

  def pipeline = foreignKey("pipeline_histories_pipelines_fk", pipelineId, TableQuery[Pipelines])(
    _.id, onUpdate = ForeignKeyAction.Restrict, onDelete = ForeignKeyAction.Cascade)

  def pk = primaryKey("pipeline_histories_pk", (pipelineId, nextRunTime))

}

object PipelineHistories {
  def apply() = TableQuery[PipelineHistories]
}
