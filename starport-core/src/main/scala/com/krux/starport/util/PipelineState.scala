package com.krux.starport.util

object PipelineState extends Enumeration {
  type State = Value
  /** Alas, AWS never publishes the exhaustive list of states.
    * The states here are learned from our past errors and AWS documentation at
    * http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-pipeline-status.html
   */
  val ACTIVATING, CANCELED, CASCADE_FAILED, DEACTIVATING, DELETING, FAILED, FINISHED, INACTIVE, PAUSED, PENDING,
    RUNNING, SCHEDULING, SCHEDULED, SHUTTING_DOWN, SKIPPED, TIMEDOUT, VALIDATING,
    WAITING_FOR_RUNNER, WAITING_ON_DEPENDENCIES = Value
}


