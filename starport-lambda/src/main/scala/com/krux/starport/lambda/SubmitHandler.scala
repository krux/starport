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

//sealed case class ExitException(status: Int) extends SecurityException("System.exit() called but ignored") {
//}
//
//sealed class NoExitSecurityManager extends SecurityManager {
//  override def checkPermission(perm: Permission): Unit = {}
//
//  override def checkPermission(perm: Permission, context: Object): Unit = {}
//
//  override def checkExit(status: Int): Unit = {
//    super.checkExit(status)
//    throw ExitException(status)
//  }
//}

class SubmitHandler extends RequestHandler[SubmitRequest, SubmitResponse] {

  def logger = LoggerFactory.getLogger(getClass)

  def handleRequest(input: SubmitRequest, context: Context): SubmitResponse = {
    logger.info("lambda invoked...")
    SubmitPipeline.main(input.getArgs)
    logger.info("lambda finished...")
    SubmitResponse("com.krux.starport.lambda.SubmitHandler processed: ", input)
  }
}

