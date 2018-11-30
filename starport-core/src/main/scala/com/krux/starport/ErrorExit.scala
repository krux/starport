package com.krux.starport

import org.slf4j.Logger

/**
 * Exists with the correct error code
 */
object ErrorExit {

  def invalidCommandlineArguments(logger: Logger): Unit = {
    logger.error("Invalid commandline arguments")
    System.exit(1)
  }

  def pipelineAlreadyExists(logger: Logger): Unit = {
    logger.error("Pipeline already exists")
    System.exit(2)
  }

  def pipelineDoesNotExist(logger: Logger): Unit = {
    logger.error("Pipeline does not exist")
    System.exit(3)
  }

  def invalidPipelineEnableOptions(logger: Logger): Unit = {
    logger.error("You cannot enable and disable a pipeline at the same time")
    System.exit(4)
  }

}
