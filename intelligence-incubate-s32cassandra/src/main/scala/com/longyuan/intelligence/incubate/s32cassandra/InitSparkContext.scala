package com.longyuan.intelligence.incubate.s32cassandra

import java.io.BufferedInputStream
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by mulin on 2017/12/8.
  */
class InitSparkContext {
  private var _conf: SparkConf = _
  private var _sqlContext: SparkSession = _
  private var _sparkContext: SparkContext = _

  private val properties: Properties = new Properties

  private def loadProperties: Unit  = {
    try {
      val driverIn = getClass.getResourceAsStream("/driver.properties")
      val awsIn = getClass.getResourceAsStream("/aws.properties")
      val s3In = getClass.getResourceAsStream("/s3.properties")
      val cassandraIn = getClass.getResourceAsStream("/cassandra.properties")

      properties.load(new BufferedInputStream(driverIn))
      properties.load(new BufferedInputStream(awsIn))
      properties.load(new BufferedInputStream(s3In))
      properties.load(new BufferedInputStream(cassandraIn))

    }catch {
      case e: Exception => println(s"load properties exception: ${e}" )
    }
  }
  loadProperties

  val master = properties.getProperty("driver.master", "local[*]")

  //aws
  private val accessKey = properties.getProperty("aws.accessKey", "AKIAO4RERNKGOMSUNZVA")
  private val secretKey = properties.getProperty("aws.secretKey", "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")

  //s3
  private val _bucket = properties.getProperty("aws.s3.bucket", "csdatas3production")
  private val _appId = properties.getProperty("aws.s3.appId", "bc4455c1f7a9c0f7")

  private val s3Endpoint = properties.getProperty("aws.s3.endpoint", "s3.cn-north-1.amazonaws.com.cn")

  //cassandra
  private val cassandra_host = properties.getProperty("cassandra.host", "172.16.249.39")

  _conf = {
    val tempConf = new SparkConf()
      .setAppName("S32CassandraDriver")
      .set("spark.cassandra.connection.host", cassandra_host)
    if ("local[*]".equals(master)) {
      tempConf.setMaster(master)
    }
    tempConf
  }

  /**
    * 初始化 Cassandra 配置
    */
  private def initCassandraConf: Unit = {
    if (null != _conf) {
      _conf.set("connection.keep_alive_ms", "20000")
      _conf.set("connection.timeout_ms", "20000")
    }
  }

  initCassandraConf

  _sqlContext = SparkSession
    .builder
    .config(_conf)
    .getOrCreate()
  _sparkContext = _sqlContext.sparkContext

  private def initAWSHadoopConf: Unit = {
    if (null != _sparkContext) {
      _sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
      _sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
      _sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3Endpoint)
    }
  }

  initAWSHadoopConf


  def sparkContext: SparkContext = _sparkContext
  def sqlContext: SparkSession = _sqlContext

  def s3Bucket: String = _bucket
  def s3AppId: String = _appId
}
