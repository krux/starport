package com.krux.starport.util

import java.security.Permission

case class LambdaExitException(status: Int) extends Exception("[system exit disallowed in AWS Lambda runtime]") {}

/**
  * Override SecurityManager when executing via AWS Lambda, in order to "catch" sys.exit.
  */
class LambdaNoExitSecurityManager extends SecurityManager {
  override def checkPermission(perm: Permission): Unit = {}
  override def checkPermission(perm: Permission, context: Object): Unit = {}
  override def checkExit(status: Int): Unit = {
    if(Lambda.isLambda()) {
      throw LambdaExitException(status)
    } else {
      super.checkExit(status)
    }
  }
}

object Lambda {
  /**
    * Used to detect if the execution context is that of AWS lambda.
    *
    * -Dexecution.context=lambda property must be set (JAVA_TOOL_OPTIONS environment variable in the AWS lambda console)
    *
    * @return
    */
  def isLambda(): Boolean = {
    System.getProperty("execution.context") == "lambda"
  }
}
