package com.kakao.cuesheet

import javax.servlet.http.HttpServletRequest

import com.kakao.mango.concurrent.NamedExecutors
import com.kakao.mango.logging.Logging
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.ui.{CustomPage, SparkUIHook}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.xml.Node

/** Added a tab in the Web UI to stop the application */
class StopTab(val sc: SparkContext) extends Logging {

  var ssc: StreamingContext = _

  var stopped: Boolean = false

  def relativePath(request: HttpServletRequest) = request.getRequestURI.substring(request.getContextPath.length)

  val page = new CustomPage("stop") {
    override def renderPage(request: HttpServletRequest): Seq[Node] = relativePath(request) match {
      case "/" =>
        <p>Click one of the buttons below to stop this application:</p>
          <div class="row-fluid">
            <div class="span4">
              <button onclick={s"if(confirm('Are you sure to stop the spark context?')) { location='confirm'; }"}
                      class="btn" style="width: 100%; height: 50px; font-size: 18pt;">
                Stop Normally
              </button>
              <p></p>
              <p>
                This will try to stop the SparkContext (and StreamingContext if applicable) normally.
                In Spark Streaming applications it will wait until all currently queued minibatches are completed.
              </p>
            </div>
            <div class="span4">
              <button onclick={s"if(confirm('Are you sure to force stop the spark context?')) { location='force'; }"}
                      class="btn btn-warning" style="width: 100%; height: 50px; font-size: 18pt;">
                Force Stop
              </button>
              <p></p>
              <p>
                This will force stop the SparkContext (and StreamingContext if applicable, with stopGracefully=false).
                Currently running Spark Streaming minibatches might not complete if you choose this option.
              </p>
            </div>
            <div class="span4">
              <button onclick={s"if(confirm('Are you sure to kill this local application?')) { location='exit'; }"}
                      class="btn btn-danger" style="width: 100%; height: 50px; font-size: 18pt;">
                Kill The Spark Driver
              </button>
              <p></p>
              <p>
                This will call Runtime.getRuntime.halt(-1) to stop the driver process.
              </p>
            </div>
          </div>
      case "/stopped" =>
        if (stopped) {
          <div>
            <h1>This application has been stopped!</h1>
          </div>
        } else {
          <script>{"location='..';"}</script>
        }
      case "/error" =>
        if (stopped) {
          <div>
            <h1>This application has been stopped!</h1>
          </div>
        } else {
          <script>{"location='..';"}</script>
        }
    }
  }

  // add this page to Spark UI
  SparkUIHook(sc).add(page)

  page.attachRedirectHandler(s"/${page.name}/confirm", s"/${page.name}/stopped", handler(stopGracefully = true))
  page.attachRedirectHandler(s"/${page.name}/force", s"/${page.name}/stopped", handler(stopGracefully = false))
  page.attachRedirectHandler(s"/${page.name}/exit", s"/${page.name}/stopped", exit)

  val scheduler = NamedExecutors.scheduled("stop-tab")

  /** Safely stop this application using the stop() methods of SparkContext and StreamingContext.
    * It is a safer way to do, but the reponse may be slow.
    */
  def handler(stopGracefully: Boolean)(request: HttpServletRequest): Unit = {
    logger.info(s"Stop requested from ${request.getRemoteAddr} via Web UI; graceful=$stopGracefully")
    stopped = true
    scheduler.scheduleIn(500 millis) {
      logger.info(s"Stopping the application...")
      try {
        stop(stopGracefully)
      } catch {
        case e: InterruptedException =>
          // ignore Jetty interrupted exception
          logger.info(s"Ignoring Interrupted Exception : ${e.getMessage}")
      }
      logger.info(s"The SparkContext has been successfully stopped!")
    }
  }

  def stop(stopGracefully: Boolean = true): Unit = {
    logger.info(s"Stop requested; sc=$sc ssc=$ssc graceful=$stopGracefully")
    ssc match {
      case _: StreamingContext => ssc.stop(stopSparkContext = true, stopGracefully)
      case _ => sc.stop()
    }
  }

  /** abruptly halt this JVM, using Runtime.halt() */
  def exit(request: HttpServletRequest): Unit = {
    logger.info(s"Exit requested from ${request.getRemoteAddr} via Web UI")
    stopped = true
    scheduler.scheduleIn(500 millis) {
      try {
        Runtime.getRuntime.halt(-1)
      } catch {
        case e: Throwable =>
          logger.info(s"Error while exiting the application: ${e.getMessage}", e)
      }
      logger.info(s"The application has been exited")
    }
  }

}

object StopTab {
  def apply(sc: SparkContext) = new StopTab(sc)
}