package com.krux.starport.util

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.net.URI

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.krux.starport.Logging

object S3FileHandler extends Logging {

  val bufferSize = 1024
  final val TmpDirectory = "/tmp/starport"

  def getTempDirectory(baseDir: Option[String]): File = {
    if (Lambda.isLambda()) {
      new File(TmpDirectory)
    } else {
      // TODO change this to "/mnt/tmp/starport"
      // and delete the file afterwards
      val tempDir = new File(baseDir.getOrElse(s"${System.getProperty("user.home")}/.starport"))
      if (!tempDir.exists()) tempDir.mkdir()
      tempDir
    }
  }

  def getFileFromS3(s3spec: String, baseDir: Option[String] = None): File = {

    val s3Uri = new URI(s3spec)
    require(s3Uri.getScheme == "s3", "must use proper s3 spec")

    val bucket = s3Uri.getHost()
    val key = s3Uri.getPath().stripPrefix("/")
    val localFile = File.createTempFile("starport_", "_" + key.split('/').last, getTempDirectory(baseDir))

    logger.info(s"Downloading $s3spec to ${localFile.getAbsolutePath()}")

    val client = AmazonS3ClientBuilder.defaultClient()
    val s3InputStream = client.getObject(bucket, key).getObjectContent
    val outputStream = new BufferedOutputStream(new FileOutputStream(localFile))

    val buffer = new Array[Byte](bufferSize)

    var read = s3InputStream.read(buffer)
    while (read != -1) {
      outputStream.write(buffer, 0, read)
      read = s3InputStream.read(buffer)
    }
    outputStream.close()

    logger.info(s"Finished downloading $s3spec to ${localFile.getAbsolutePath()}")

    localFile
  }

}
