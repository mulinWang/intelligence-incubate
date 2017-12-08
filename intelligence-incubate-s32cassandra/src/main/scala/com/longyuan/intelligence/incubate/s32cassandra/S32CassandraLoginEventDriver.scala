package com.longyuan.intelligence.incubate.s32cassandra

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.{JSON, JSONObject}
import org.slf4j.LoggerFactory
import com.datastax.spark.connector._
/**
  * Created by mulin on 2017/12/4.
  */


object S32CassandraLoginEventDriver {
  @transient lazy val LOGGER = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val init = new InitSparkContext

    val sqlContext = init.sqlContext

    var date = "2017-08-01"
    var files = "login_*.log"

    if (null != args && args.length > 0) {
      date = args(0)
      if (args.length == 2) {
        files = args(1)
      }
    }
    val path = initS3Path(init, date, files)

    LOGGER.info("date:{}", date)
    LOGGER.info("files:{}", files)
    LOGGER.info("path:{}", path)

    val loginDF = sqlContext.read.json(path)
    LOGGER.info("login count:{}", loginDF.count)
    /**
      * 处理Json
      * @param iter
      * @return
      */
    def processJsonFunc(iter: Iterator[JSONObject]) : Iterator[Login] = {
      val ymdhmsDateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val ymdDateFormat: DateTimeFormatter =  DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val zoneOffset: ZoneOffset = ZoneOffset.of("+8")

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
          val ts: Long = localDateTime.toEpochSecond(zoneOffset) * 1000

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

    newLogin.saveToCassandra("miwu", "login", SomeColumns("account_id", "dt", "ts", "channel_id", "platform", "game_area", "device_id", "ip"))
  }

  /**
    * 初始化 s3 路径
    * @param date
    * @param files
    * @return
    */
  def initS3Path(init: InitSparkContext , date: String, files: String): String = {
    s"s3a://${init.s3Bucket}/${init.s3AppId}/$date/$files"
  }
  case class Login(accountId: String, dt: String, ts: Long, channelId: String,
                   platform: Int, gameArea: String, deviceId: String, ip: String)
}

