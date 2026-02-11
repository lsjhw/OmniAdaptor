package com.huawei.boostkit.spark.util

import org.apache.spark.internal.Logging

trait RewriteLogger extends Logging {

  private val logFlag = "[OmniMV]"

  // 从系统属性读取日志级别：-Domnimv.log.level=INFO
  // 默认 INFO
  private def logLevel: String =
    Option(System.getProperty("omnimv.log.level"))
      .map(_.trim.toUpperCase)
      .filter(_.nonEmpty)
      .getOrElse("INFO")

  def logBasedOnLevel(f: => String): Unit = {
    logLevel match {
      case "TRACE" => logTrace(f)
      case "DEBUG" => logDebug(f)
      case "INFO"  => logInfo(f)
      case "WARN"  => logWarning(f)
      case "ERROR" => logError(f)
      case _       => logInfo(f)
    }
  }

  def logDetail(f: => String): Unit = {
    logLevel match {
      case "ERROR" => logWarning(f)
      case _       =>
    }
  }

  override def logInfo(msg: => String): Unit = super.logInfo(s"$logFlag $msg")
  override def logDebug(msg: => String): Unit = super.logDebug(s"$logFlag $msg")
  override def logTrace(msg: => String): Unit = super.logTrace(s"$logFlag $msg")
  override def logWarning(msg: => String): Unit = super.logWarning(s"$logFlag $msg")
  override def logError(msg: => String): Unit = super.logError(s"$logFlag $msg")
}
