package com.krux.starport.lambda

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.krux.starport.Starport

class Handler extends RequestHandler[Request, Response] {

  def handleRequest(input: Request, context: Context): Response = {
    val args = new Array[String](0)
    Starport.main(args)
    Response("Starport lambda handler: ", input)
  }
}

