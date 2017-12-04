package com.longyuan.intelligence.incubate.s32cassandra

import java.io.BufferedInputStream
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import com.datastax.spark.connector._
/**
  * Created by mulin on 2017/12/4.
  */
class S32CassandraDriver {
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

  private val s3Endpoint = properties.getProperty("aws.s3.endpoint", "bc4455c1f7a9c0f7")

  //cassandra
  private val cassandra_host = properties.getProperty("cassandra.host", "52.80.0.158")

  _conf = {
    val tempConf = new SparkConf()
      .setAppName("S32CassandraDriver")
      .set("spark.cassandra.connection.host", cassandra_host)
    if ("local[*]".equals(master)) {
      tempConf.setMaster(master)
    }
    tempConf
  }

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

object S32CassandraDriver {
  def apply: S32CassandraDriver = new S32CassandraDriver()

  @transient lazy val LOGGER = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val driver = new S32CassandraDriver

    val sqlContext = driver.sqlContext

    var date = "2017-08-01"
    var files = "login_*.log"

    if (null != args && args.length > 0) {
      date = args(0)
      if (args.length == 2) {
        files = args(1)
      }
    }
    val path = initS3Path(driver, date, files)

    LOGGER.info("date:{}", date)
    LOGGER.info("files:{}", files)
    LOGGER.info("path:{}", path)

    val loginDF = sqlContext.read.json(path)

    /**
      * 处理Json
      * @param iter
      * @return
      */
    def processJsonFunc(iter: Iterator[JSONObject]) : Iterator[Login] = {
      val ymdhmsDateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val ymdDateFormat: DateTimeFormatter =  DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val zoneOffset: ZoneOffset = ZoneOffset.of("+8");

      var resultList = List[Login]()
      while (iter.hasNext)
      {
        try{
          val json = iter.next;

          val accountId: String = json.getString("accountId")
          val oldTs = json.getString("ts")

          val localDateTime: LocalDateTime =
            LocalDateTime.parse(oldTs, ymdhmsDateFormat)

          val dt: String = localDateTime.format(ymdDateFormat)
          val ts: Long = localDateTime.toEpochSecond(zoneOffset)

          val channelId: String = json.getString("channelId")
          val platform: Int = json.getIntValue("platform")
          val gameArea: String = json.getString("gameArea")
          val deviceId: String = json.getString("deviceId")
          val ip: String = json.getString("ip")

          val login = Login(accountId, dt, ts, channelId, platform, gameArea, deviceId, ip)
          resultList .::= (login)
        }catch {
          case ex: Exception => LOGGER.error("exception:{}", ex)
            resultList .::= (null)
        }
      }
      resultList.iterator
    }


    val newLogin = loginDF.toJSON.rdd.map(line => {
      try {
        val json = JSON.parseObject(line)
        json
      }catch {
        case ex: Exception => {
          println(s"exception: $ex")
        }
          null
      }
    }).filter(json => null != json &&
      "login".equals(json.getString("event"))
    ).mapPartitions(processJsonFunc)
      .filter(login => null != login)

    LOGGER.info("login rdd:{}", newLogin.take(10).mkString)
    LOGGER.info("login rdd:{}", newLogin.take(10).mkString)


    newLogin.saveToCassandra("miwu", "login", SomeColumns("account_id", "dt", "ts", "channel_id", "platform", "game_area", "device_id", "ip"))

    //    dt text,
//    account_id text,
//    ts timestamp,
//    channel_id text,
//    device_id text,
//    game_area text,
//    ip text,
//    platform int,
  //accountId, dt, ts, channelId, platform,  gameArea, deviceId, ip


  }

  /**
    * 初始化 s3 路径
    * @param date
    * @param files
    * @return
    */
  def initS3Path(driver: S32CassandraDriver , date: String, files: String): String = {
    s"s3a://${driver.s3Bucket}/${driver.s3AppId}/$date/$files"
  }

  case class Login(accountId: String, dt: String, ts: Long, channelId: String,
                   platform: Int, gameArea: String, deviceId: String, ip: String)
}


