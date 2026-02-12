package com.huawei.boostkit.spark.util

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object KerberosUtil {

  /** new configuration from spark */
  def newConfiguration(spark: SparkSession): Configuration = {
    val configuration: Configuration = spark.sessionState.newHadoopConf()
    newConfiguration(configuration, spark.sparkContext.getConf)
  }

  /** new configuration from configuration */
  def newConfiguration(configuration: Configuration, sparkConf: SparkConf): Configuration = {
    // 读取 hdfs-site/core-site
    val xmls = Seq("hdfs-site.xml", "core-site.xml")
    val xmlDir = System.getProperty("omnimv.hdfs_conf", ".")
    xmls.foreach { xml =>
      val file = new File(xmlDir, xml)
      if (file.exists()) {
        configuration.addResource(new Path(file.getAbsolutePath))
      }
    }

    // kerberos
    if ("kerberos".equalsIgnoreCase(configuration.get("hadoop.security.authentication"))) {
      val krb5Conf = System.getProperty("omnimv.krb5_conf", "/etc/krb5.conf")
      System.setProperty("java.security.krb5.conf", krb5Conf)

      // 优先读系统属性，其次读 SparkConf key（同名）
      val principal =
        Option(System.getProperty("omnimv.principal"))
          .filter(_.trim.nonEmpty)
          .orElse(sparkConf.getOption("omnimv.principal"))
          .orNull

      val keytab =
        Option(System.getProperty("omnimv.keytab"))
          .filter(_.trim.nonEmpty)
          .orElse(sparkConf.getOption("omnimv.keytab"))
          .orNull

      if (principal == null || keytab == null) {
        throw new RuntimeException("omnimv.principal or omnimv.keytab cannot be null")
      }

      UserGroupInformation.setConfiguration(configuration)
      UserGroupInformation.loginUserFromKeytab(principal, keytab)
    }

    configuration
  }
}
