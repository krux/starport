package com.krux.starport.util

import scala.concurrent.ExecutionContext

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.Logging
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.WaitForIt
import com.krux.starport.db.record.PipelineProgress
import com.krux.starport.db.table.PipelineProgresses
import com.krux.starport.util.ProgressStatus.Progress


class PipelineProgressHelper(implicit conf: StarportSettings) extends WaitForIt with Logging {

  def db = conf.jdbc.db

  def insertOrUpdatePipelineProgress(pipelineIds: Set[Int], progress: Progress)
    (implicit ec: ExecutionContext): Unit = {

    if (pipelineIds.nonEmpty) {
      logger.info(s"Mark pipelines ${pipelineIds.mkString(", ")} as $progress ...")

      val toBeUpdated = PipelineProgresses().filter(_.pipelineId.inSet(pipelineIds))

      val actions = (
        for {
          existing <- toBeUpdated.map(_.pipelineId).result
          toBeInserted = pipelineIds
            .filterNot(existing.contains(_))
            .map { pId =>
              PipelineProgress(
                pId,
                progress.toString
              )
            }
          updated <- toBeUpdated.map(_.progress).update(progress.toString)
          result <- PipelineProgresses() ++= toBeInserted
        } yield result).transactionally

      db.run(actions).waitForResult
    }
  }

  def insertOrUpdatePipelineProgress(pipelineId: Int, progress: Progress): Unit = {
    logger.info(s"Mark pipeline $pipelineId as $progress ...")

    db.run(
      PipelineProgresses().insertOrUpdate(PipelineProgress(pipelineId, progress.toString))
    ).waitForResult
  }
}
