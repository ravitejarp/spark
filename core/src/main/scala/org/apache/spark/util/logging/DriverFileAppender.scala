/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.logging

import java.io.{File, FileOutputStream, InputStream, IOException}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.{IntParam, Utils}

/**
 * Continuously appends the data from an input stream into the given file.
 */
private[spark] class DriverFileAppender(inputStream: InputStream, file: File, bufferSize: Int = 8192)
  extends FileAppender {
  @volatile private var outputStream: FileOutputStream = null
  @volatile private var markedForStop = false     // has the appender been asked to stopped

  // Thread that reads the input stream and writes to file
  private val writingThread = new Thread("File appending thread for " + file) {
    setDaemon(true)
    override def run() {
      Utils.logUncaughtExceptions {
        appendStreamToFile()
      }
    }
  }
  writingThread.start()

  /**
   * Wait for the appender to stop appending, either because input stream is closed
   * or because of any error in appending
   */
  def awaitTermination() {
    writingThread.join()
  }

  /** Stop the appender */
  def stop() {
    markedForStop = true
  }

  /** Continuously read chunks from the input stream and append to the file */
  protected def appendStreamToFile() {
    try {
      logDebug("Started appending thread")
      Utils.tryWithSafeFinally {
        openFile()
        val buf = new Array[Byte](bufferSize)
        var n = 0
        while (!markedForStop && n != -1) {
          try {
            n = inputStream.read(buf)
          } catch {
            // An InputStream can throw IOException during read if the stream is closed
            // asynchronously, so once appender has been flagged to stop these will be ignored
            case _: IOException if markedForStop =>  // do nothing and proceed to stop appending
          }
          if (n > 0) {
            appendToFile(buf, n)
          }
        }
      } {
        closeFile()
      }
    } catch {
      case e: Exception =>
        logError(s"Error writing stream to file $file", e)
    }
  }

  /** Append bytes to the file output stream */
  protected def appendToFile(bytes: Array[Byte], len: Int) {
    if (outputStream == null) {
      openFile()
    }
    outputStream.write(bytes, 0, len)
  }

  /** Open the file output stream */
  protected def openFile() {
    outputStream = new FileOutputStream(file, true)
    logDebug(s"Opened file $file")
  }

  /** Close the file output stream */
  protected def closeFile() {
    outputStream.flush()
    outputStream.close()
    logDebug(s"Closed file $file")
  }
}

/**
 * Companion object to [[org.apache.spark.util.logging.FileAppender]] which has helper
 * functions to choose the correct type of FileAppender based on SparkConf configuration.
 */
private[spark] object DriverFileAppender extends Logging {

  /** Create the right appender based on Spark configuration */
  def apply(inputStream: InputStream, file: File, conf: SparkConf): DriverFileAppender = {

    import RollingFileAppender._

    val driverRollingStrategy = conf.get(DRIVER_STRATEGY_PROPERTY, DRIVER_STRATEGY_DEFAULT)
    val driverRollingSizeBytes = conf.get(DRIVER_SIZE_PROPERTY, DRIVER_STRATEGY_DEFAULT)
    val driverRollingInterval = conf.get(DRIVER_INTERVAL_PROPERTY, DRIVER_INTERVAL_DEFAULT)

    def createTimeBasedAppenderForDriver(): DriverFileAppender = {
      val validatedParams: Option[(Long, String)] = driverRollingInterval match {
        case "daily" =>
          logInfo(s"Rolling executor logs enabled for $file with daily rolling")
          Some((24 * 60 * 60 * 1000L, "--yyyy-MM-dd"))
        case "hourly" =>
          logInfo(s"Rolling executor logs enabled for $file with hourly rolling")
          Some((60 * 60 * 1000L, "--yyyy-MM-dd--HH"))
        case "minutely" =>
          logInfo(s"Rolling executor logs enabled for $file with rolling every minute")
          Some((60 * 1000L, "--yyyy-MM-dd--HH-mm"))
        case IntParam(seconds) =>
          logInfo(s"Rolling executor logs enabled for $file with rolling $seconds seconds")
          Some((seconds * 1000L, "--yyyy-MM-dd--HH-mm-ss"))
        case _ =>
          logWarning(s"Illegal interval for rolling executor logs [$driverRollingInterval], " +
            s"rolling logs not enabled")
          None
      }
      validatedParams.map {
        case (interval, pattern) =>
          new DriverRollingFileAppender(
            inputStream, file, new TimeBasedRollingPolicy(interval, pattern), conf)
      }.getOrElse {
        new DriverFileAppender(inputStream, file)
      }
    }

    def createSizeBasedAppenderForDriver(): DriverFileAppender = {
      driverRollingSizeBytes match {
        case IntParam(bytes) =>
          logInfo(s"Rolling executor logs enabled for $file with rolling every $bytes bytes")
          new DriverRollingFileAppender(inputStream, file, new SizeBasedRollingPolicy(bytes), conf)
        case _ =>
          logWarning(
            s"Illegal size [$driverRollingSizeBytes] for rolling executor logs, rolling logs not enabled")
          new DriverFileAppender(inputStream, file)
      }
    }

    driverRollingStrategy match {
      case "" =>
        new DriverFileAppender(inputStream, file)
      case "time" =>
        createTimeBasedAppenderForDriver()
      case "size" =>
        createSizeBasedAppenderForDriver()
      case _ =>
        logWarning(
          s"Illegal strategy [$driverRollingStrategy] for rolling executor logs, " +
            s"rolling logs not enabled")
        new DriverFileAppender(inputStream, file)
    }
  }
}

