package com.krux.starport.lambda

import java.io.ByteArrayOutputStream
import java.io.PrintStream

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.krux.starport.Logging
import com.krux.starport.db.tool.SubmitPipeline
import com.krux.starport.util.LambdaExitException

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
class SubmitHandler extends RequestHandler[SubmitRequest, SubmitResponse] with Logging {

  def handleRequest(input: SubmitRequest, context: Context): SubmitResponse = {
    val outCapture = new ByteArrayOutputStream
    val errCapture = new ByteArrayOutputStream
    val outPrintStream = new PrintStream(outCapture)
    val errPrintStream = new PrintStream(errCapture)
    val origOut = System.out
    val origErr = System.err
    var status: Int = 0

    try {
      System.setErr(errPrintStream)
      System.setOut(outPrintStream)
      SubmitPipeline.main(input.getArgs)
    } catch {
      case caughtExit: LambdaExitException => {
        status = caughtExit.status
        logger.error("exit:", caughtExit)
      }
      case unhandled: Throwable =>  {
        status = 255
        logger.error("exception:",unhandled)
      }
    } finally {
      outPrintStream.flush()
      outPrintStream.close()
      errPrintStream.flush()
      errPrintStream.close()
      System.setErr(origErr)
      System.setOut(origOut)
    }

    //This makes sure the output is also sent to the AWS lambda runtime so it can be
    //pushed into CloudWatch.
    if (outCapture.size()>0) logger.info(outCapture.toString)
    if (errCapture.size()>0) logger.error(errCapture.toString)

    SubmitResponse(outCapture.toString, errCapture.toString, status, input)
  }
}

