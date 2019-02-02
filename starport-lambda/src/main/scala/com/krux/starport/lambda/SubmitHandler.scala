package com.krux.starport.lambda

import java.io.ByteArrayOutputStream
import java.security.Permission

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.krux.starport.db.tool.SubmitPipeline
import org.slf4j.LoggerFactory


sealed case class ExitException(status: Int) extends SecurityException("System.exit() called but ignored") {
}

sealed class NoExitSecurityManager extends SecurityManager {
  override def checkPermission(perm: Permission): Unit = {}

  override def checkPermission(perm: Permission, context: Object): Unit = {}

  override def checkExit(status: Int): Unit = {
    super.checkExit(status)
    throw ExitException(status)
  }
}

/**
  * Lambda to invoke the com.krux.starport.db.tool.SubmitPipeline util remotely.
  *
  * -Dstarport.config.url=s3://{your-bucket}/starport/starport.conf
  *
  */
class SubmitHandler extends RequestHandler[SubmitRequest, SubmitResponse] {

  def logger = LoggerFactory.getLogger(getClass)

  def handleRequest(input: SubmitRequest, context: Context): SubmitResponse = {
    val outCapture = new ByteArrayOutputStream
    val errCapture = new ByteArrayOutputStream
    logger.info("lambda invoked...")
    try {
      Console.withOut(outCapture) {
        Console.withErr(errCapture) {
          SubmitPipeline.main(input.getArgs)
        }
      }
    } catch {
      case _: Throwable => logger.error("lambda error",_)
    } finally {
      logger.info("lambda finished...")
    }
    SubmitResponse(outCapture.toString, errCapture.toString, input)
  }
}

