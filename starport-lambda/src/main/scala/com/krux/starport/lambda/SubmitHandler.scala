package com.krux.starport.lambda

import java.io.ByteArrayOutputStream

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.krux.starport.db.tool.SubmitPipeline
import org.slf4j.LoggerFactory

/**
  * Lambda to invoke the com.krux.starport.db.tool.SubmitPipeline util remotely.
  *
  * Property configuration for lambda (pass via JAVA_TOOL_OPTIONS env var):
  *
  * -Dstarport.config.url=s3://{your-bucket}/starport/starport.conf
  * -Dlogger.root.level=DEBUG|INFO|...
  * -Dexecution.context=lambda
  *
  */
class SubmitHandler extends RequestHandler[SubmitRequest, SubmitResponse] {

  def logger = LoggerFactory.getLogger(getClass)

  def handleRequest(input: SubmitRequest, context: Context): SubmitResponse = {
    val outCapture = new ByteArrayOutputStream
    val errCapture = new ByteArrayOutputStream
    //      Console.withOut(outCapture) {
    //        Console.withErr(errCapture) {
    logger.info("lambda invoked...")
    try {
      SubmitPipeline.main(input.getArgs)
    } catch {
      case unknown: Throwable => logger.error("unhandled error", unknown)
    } finally {
      logger.info("lambda finished...")
    }
    SubmitResponse(outCapture.toString, errCapture.toString, input)
  }
}

