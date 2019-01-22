package com.krux.starport.lambda

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.krux.starport.Starport
import org.slf4j.LoggerFactory

/**
  * Lambda to invoke the com.krux.starport.Starport service main
  *
  * -Dstarport.config.url=s3://{your-bucket}/starport/starport.conf
  *
  */
class ServiceHandler extends RequestHandler[ServiceRequest, ServiceResponse] {

  def logger = LoggerFactory.getLogger(getClass)

  def handleRequest(input: ServiceRequest, context: Context): ServiceResponse = {
    logger.info("lambda invoked...")
    Starport.main(input.getArgs)
    logger.info("lambda finished...")
    ServiceResponse("com.krux.starport.lambda.ServiceHandler processed: ", input)
  }
}

