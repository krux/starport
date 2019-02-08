package com.krux.starport.lambda

import java.io.ByteArrayOutputStream
import java.io.PrintStream

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

    logger.info("lambda v05 invoked...")

    Console.withOut(outCapture) {
      Console.withErr(errCapture) {
        try {
          SubmitPipeline.main(input.getArgs)
        } catch {
          case unknown: Throwable => {
            val ps = new PrintStream(errCapture)
            unknown.printStackTrace(ps)
            ps.close
          }
        }
      }
    }

    logger.info("lambda v05 finished...")
    outCapture.flush()
    errCapture.flush()
    if (outCapture.size()>0) logger.info(outCapture.toString)
    if (errCapture.size()>0) logger.error(errCapture.toString)
    SubmitResponse(outCapture.toString, errCapture.toString, input)
  }
}

