package com.remarkmedia.spark.util

import org.slf4j.LoggerFactory

trait Logs {
  val logger = LoggerFactory.getLogger(this.getClass)

  def debug(message: => String) = logger.debug(message)

  def debug(message: => String, ex: Throwable) = logger.debug(message, ex)

  def info(message: => String) = logger.info(message)

  def info(message: => String, ex: Throwable) = logger.info(message, ex)

  def warn(message: => String) = logger.warn(message)

  def warn(message: => String, ex: Throwable) = logger.warn(message, ex)

  def error(ex: Throwable) = logger.error(ex.toString, ex)

  def error(message: => String) = logger.error(message)

  def error(message: => String, ex: Throwable) = logger.error(message, ex)
}
