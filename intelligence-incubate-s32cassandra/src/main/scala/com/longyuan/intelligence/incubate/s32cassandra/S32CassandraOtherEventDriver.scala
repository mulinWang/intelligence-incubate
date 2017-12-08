package com.longyuan.intelligence.incubate.s32cassandra

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import com.datastax.spark.connector.SomeColumns
import org.slf4j.LoggerFactory
import com.datastax.spark.connector._

/**
  * Created by mulin on 2017/12/8.
  */
  object S32CassandraOtherEventDriver {
  @transient lazy val LOGGER = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val init = new InitSparkContext

    val sqlContext = init.sqlContext

    var date = "2017-08-01"
    var files = "other_*.log"

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

    val otherDF = sqlContext.read.json(path)
    LOGGER.info("other count:{}", otherDF.count)
    /**
      * 处理Json
      * @param iter
      * @return
      */
    def processJsonFunc(iter: Iterator[JSONObject]) : Iterator[Other] = {
      val ymdhmsDateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val ymdDateFormat: DateTimeFormatter =  DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val zoneOffset: ZoneOffset = ZoneOffset.of("+8")

      var resultList = List[Other]()
      while (iter.hasNext)
      {
        try{
          val json = iter.next

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
          val otherEvent: String = json.getString("otherEvent")

          val otherData: String = {
            try {
             val otherDataJson = json.getJSONObject("data")
              if (null != otherDataJson) otherDataJson.toJSONString
              else "{}"
            }catch {
              case jsonEx: JSONException => LOGGER.error("json parse exception:{}", jsonEx)
                "{}"
            }
          }
//          LOGGER.info("otherData:{}", otherData)

          val other: Other = Other(accountId, dt, ts, channelId, platform, gameArea, deviceId, ip, otherEvent, otherData)
          resultList .::= (other)
        }catch {
          case ex: Exception => LOGGER.error("exception:{}", ex)
            resultList .::= (null)
        }
      }
      resultList.iterator
    }

    val newOther = otherDF.toJSON.rdd.map(line => {
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
      "other".equals(json.getString("event"))
    ).mapPartitions(processJsonFunc)
      .filter(Other => null != Other)

    LOGGER.info("other rdd:{}", newOther.take(10).mkString)

    newOther.saveToCassandra("miwu", "other", SomeColumns("account_id", "dt", "ts", "channel_id",
      "platform", "game_area", "device_id", "ip", "other_event", "other_data"))
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

  case class Other(accountId: String, dt: String, ts: Long, channelId: String, platform: Int,
                   gameArea: String, deviceId: String, ip: String, otherEvent: String, otherData: String)

}

