package com.krux.starport.lambda

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.krux.starport.db.tool.SubmitPipeline
import org.slf4j.LoggerFactory

/**
  * Lambda to invoke the com.krux.starport.db.tool.SubmitPipeline util.
  *
  * -Dstarport.config.url=s3://{your-bucket}/starport/starport.conf
  *
  */
class SubmitHandler extends RequestHandler[SubmitRequest, SubmitResponse] {

  def logger = LoggerFactory.getLogger(getClass)

  def handleRequest(input: SubmitRequest, context: Context): SubmitResponse = {
    logger.info("lambda invoked...")
    SubmitPipeline.main(input.getArgs)
    logger.info("lambda finished...")
    SubmitResponse("Starport lambda handler: ", input)
  }
}

