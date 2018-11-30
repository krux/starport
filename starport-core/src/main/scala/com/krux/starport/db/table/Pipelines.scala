package com.krux.starport.db.table

import com.github.nscala_time.time.Imports.DateTime
import slick.jdbc.PostgresProfile.api._

import com.krux.starport.db.DateTimeMapped
import com.krux.starport.db.record.Pipeline


/**
 * @note when scheduling, the type is always assumed to be cron
 */
class Pipelines(tag: Tag) extends Table[Pipeline](tag, "pipelines") with DateTimeMapped {

  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

  /**
   * The pieline name (usually the same as the class)
   */
  def name = column[String]("name", O.SqlType("VARCHAR(200)"))

  /**
   * where the jar that defines the pipeline is (e.g. "s3://krux-temp/jars/xxx.jar")
   */
  def jar = column[String]("jar", O.SqlType("VARCHAR(200)"))

  /**
   * The class name of the pipeline
   */
  def `class` = column[String]("class", O.SqlType("VARCHAR(200)"))

  /**
   * flag for temporarily deactivate a pipeline
   */
  def isActive = column[Boolean]("is_active")

  /**
   * number of past pipelines in UI to keep before delete
   */
  def retention = column[Int]("retention")

  /**
   * The actual start time
   */
  def start = column[DateTime]("start")

  /**
   * The duration period
   */
  def period = column[String]("period", O.SqlType("VARCHAR(50)"))

  /**
   * When the pipline scheduling finishes, None if it runs forever
   */
  def end = column[Option[DateTime]]("end")

  /**
   * The next time it should run, None if it should not run
   */
  def nextRunTime = column[Option[DateTime]]("next_run_time")

  /**
   * Whether the pipeline should perform back fill between the previous next run and current time
   */
  def backfill = column[Boolean]("backfill", O.Default(true))

  /**
   * The owner's email, used to send failure alerts when pipeline scheduling fail
   */
  def owner = column[Option[String]]("owner", O.Default(None), O.SqlType("VARCHAR(254)"))

  def * = (id.?, name, jar, `class`, isActive, retention, start, period, end, nextRunTime, backfill, owner) <>
    (Pipeline.tupled, Pipeline.unapply)

}

object Pipelines {
  def apply() = TableQuery[Pipelines]
}
