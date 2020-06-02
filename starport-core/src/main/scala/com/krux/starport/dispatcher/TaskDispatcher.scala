package com.krux.starport.dispatcher

import com.krux.starport.cli.SchedulerOptions
import com.krux.starport.db.record.{Pipeline, ScheduledPipeline}
import com.krux.starport.exception.StarportException
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.ScheduledPipeline

trait TaskDispatcher {

  def dispatch(pipeline: Pipeline, options: SchedulerOptions, jar: String, conf: StarportSettings): Either[StarportException, Boolean]

  def retrieve(conf: StarportSettings): Seq[ScheduledPipeline]

}

