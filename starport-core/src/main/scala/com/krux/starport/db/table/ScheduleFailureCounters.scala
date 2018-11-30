package com.krux.starport.db.table

import org.joda.time.DateTime
import slick.jdbc.PostgresProfile.api._

import com.krux.starport.db.DateTimeMapped
import com.krux.starport.db.record.ScheduleFailureCounter


class ScheduleFailureCounters(tag: Tag)
  extends Table[ScheduleFailureCounter](tag, "schedule_failure_counters")
  with DateTimeMapped {

  def pipelineId = column[Int]("pipeline_id", O.PrimaryKey)

  def failureCount = column[Int]("failure_count")

  def updatedAt = column[DateTime]("updated_at")

  def * = (pipelineId, failureCount, updatedAt) <>
    (ScheduleFailureCounter.tupled, ScheduleFailureCounter.unapply)

  def pipeline = foreignKey(
      "schedule_failure_counters_pipelines_fk", pipelineId, TableQuery[Pipelines]
    )(_.id, onUpdate = ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

}

object ScheduleFailureCounters {
  def apply() = TableQuery[ScheduleFailureCounters]
}
