package com.krux.starport.db.table

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.db.DateTimeMapped
import com.krux.starport.db.record.PipelineProgress


class PipelineProgresses(tag: Tag) extends Table[PipelineProgress](tag, "pipeline_progresses") with DateTimeMapped {

  def pipelineId = column[Int]("pipeline_id")

  def progress = column[String]("progress", O.SqlType("VARCHAR(40)"))

  def * = (pipelineId, progress) <> (PipelineProgress.tupled, PipelineProgress.unapply)

  def pipeline = foreignKey("pipeline_progresses_pipelines_fk", pipelineId, TableQuery[Pipelines])(
    _.id, onUpdate = ForeignKeyAction.Restrict, onDelete = ForeignKeyAction.Cascade)

  def pk = primaryKey("pipeline_progresses_pk", pipelineId)

}

object PipelineProgresses {
  def apply() = TableQuery[PipelineProgresses]
}
