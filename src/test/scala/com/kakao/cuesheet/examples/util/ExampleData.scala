package com.kakao.cuesheet.examples.util

import java.io.FileOutputStream

import com.google.common.io.{ByteStreams, Files}

import scala.util.control.NonFatal

object ExampleData {
  lazy val path: String = {
    try {
      val resource = "data.tsv"
      val tmpfile = Files.createTempDir().getAbsolutePath + resource
      val input = getClass.getResourceAsStream(resource)
      val output = new FileOutputStream(tmpfile)
      ByteStreams.copy(input, output)
      input.close()
      output.close()
      tmpfile
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException("Could not copy example data file to temp directory", e)
    }
  }
}
