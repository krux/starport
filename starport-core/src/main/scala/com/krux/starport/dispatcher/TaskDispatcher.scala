package com.krux.starport.dispatcher

import com.krux.starport.cli.SchedulerOptions
import com.krux.starport.db.record.{Pipeline, ScheduledPipeline}
import com.krux.starport.exception.StarportException
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.ScheduledPipeline

trait TaskDispatcher {

  /**
   * This method deploys and activates a Pipeline object using hyperion cli.
   * It should be implemented in a thread safe manner as dispatch tasks can be executed in parallel
   * @param pipeline
   * @param options
   * @param jar
   * @param conf
   * @return Unit - This is a side effect function
   */
  def dispatch(pipeline: Pipeline, options: SchedulerOptions, jar: String, conf: StarportSettings): Either[StarportException, Unit]

  /**
   * This method retrieves all the pipeline Ids for the the pipelines deployed via dispatch.
   * It is meant to be invoked before starting task dispatching or after all the tasks have been dispatched.
   * This operation is not meant to be Threadsafe and should not be executed in parallel.
   * @param conf
   * @return Seq of all ScheduledPipelines
   */
  def retrieve(conf: StarportSettings): Seq[ScheduledPipeline]

}

