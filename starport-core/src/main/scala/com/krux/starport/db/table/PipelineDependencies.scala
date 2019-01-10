package com.krux.starport.db.table

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.db.record.PipelineDependency


class PipelineDependencies(tag: Tag) extends Table[PipelineDependency](tag, "pipeline_dependencies") {

  def pipelineId = column[Int]("pipeline_id")

  def upstreamPipelineId = column[Int]("upstream_pipeline_id")

  def * = (pipelineId, upstreamPipelineId) <>
    (PipelineDependency.tupled, PipelineDependency.unapply)

  def pipeline = foreignKey("pipeline_dependencies_pipelines_fk", pipelineId, TableQuery[Pipelines])(
    _.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

  def dependentPipeline = foreignKey("pipeline_dependencies_pipelines_dependency_fk", upstreamPipelineId, TableQuery[Pipelines])(
    _.id, onUpdate = ForeignKeyAction.Restrict, onDelete = ForeignKeyAction.Cascade)

  def pk = primaryKey("pipeline_dependencies_pk", (pipelineId, upstreamPipelineId))

}

object PipelineDependencies {
  def apply() = TableQuery[PipelineDependencies]
}
