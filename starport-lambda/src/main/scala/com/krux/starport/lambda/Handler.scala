package com.krux.starport.lambda

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.krux.starport.Starport
import org.slf4j.LoggerFactory

class Handler extends RequestHandler[Request, Response] {

  def logger = LoggerFactory.getLogger(getClass)

  def handleRequest(input: Request, context: Context): Response = {
    logger.info("lambda invoked...")
    val args = new Array[String](0)
    Starport.main(args)
    Response("Starport lambda handler: ", input)
  }
}

