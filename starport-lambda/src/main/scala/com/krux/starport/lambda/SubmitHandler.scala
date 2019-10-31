package com.krux.starport.lambda

import java.io.{ByteArrayOutputStream, File, PrintStream}

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
  val outCapture = new ByteArrayOutputStream
  val errCapture = new ByteArrayOutputStream
  val outPrintStream = new PrintStream(outCapture)
  val errPrintStream = new PrintStream(errCapture)
  val lambdaOut = System.out
  val lambdaErr = System.err
  System.setErr(errPrintStream)
  System.setOut(outPrintStream)

  def handleRequest(input: SubmitRequest, context: Context): SubmitResponse = {
    var status: Int = 0
    var outString = ""
    var errString = ""

    try {
      SubmitPipeline.main(input.getArgs)
    } catch {
      case caughtExit: LambdaExitException => {
        status = caughtExit.status
        logger.error("exit:", caughtExit)
        logger.error(scanTmpFiles())
      }
      case unhandled: Throwable =>  {
        status = 255
        logger.error("exception:", unhandled)
        logger.error(scanTmpFiles())
      }
    } finally {
      outPrintStream.flush()
      errPrintStream.flush()
      outString = outCapture.toString
      errString = errCapture.toString
      outCapture.reset()
      errCapture.reset()
      lambdaOut.print(outString)
      lambdaErr.print(errString)
    }

    SubmitResponse(outString, errString, status, input)
  }

  /**
   * @return /tmp file listing with size for troubleshooting purposes
   */
  private def scanTmpFiles(): String = {
    new File("/tmp")
      .listFiles()
      .map(f => s"${f.getAbsolutePath}: ${f.length()}")
      .mkString("\n")
  }
}

