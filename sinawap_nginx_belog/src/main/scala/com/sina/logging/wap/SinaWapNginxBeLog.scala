package com.sina.logging.wap

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by jianping on 2017/5/23.
  */
object SinaWapNginxBeLog {
  def cleanRecords(records:InputDStream[(String,String)]):DStream[(Date,Int,String,Double,String,String)]={
    records
      .map(line =>
      {
        val parts1 = line._2.split("`")
        val parts2 = line._2.split("\\|")

        var status: Int = 1000
        var requestTime: Double = 0.0
        var timestamp: Date = new Date()
        var idc:String = ""
        var apiDomain =""
        var apiName= ""
        try {
          val timestamptmp = parts1(0).split("\\[")
          timestamp = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH).parse(timestamptmp(2).substring(0, timestamptmp(2).indexOf("]")))
          status = parts1(4).toInt
          requestTime = parts1(8).toDouble
          val backDomain = parts2(parts2.length - 2).split('.')
          idc = backDomain(backDomain.length-3)
          apiName = parts1(2).split(" ")(1).split("\\?")(0)
          apiDomain = parts1(12)
        } catch {
          case ex:Exception => /*println(line._2)*/
        }
        (timestamp,status,idc,requestTime,apiDomain,apiName)
      }
      )
  }

  def computeRequestTime(records:DStream[(Date,Int,String,Double,String,String)]):DStream[Map[String,Any]]={
    records
      .map(t=>((t._2,t._3),t._4))
      .groupByKey()
      .map(t =>
      {
        val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
        val timestamp =dateFormat.parse(dateFormat.format(new Date()))

        Map(
          "@timestamp" -> timestamp,
          "status" -> t._1._1,
          "idc" -> t._1._2,
          "request" -> t._2.reduce(_+_)/t._2.toList.length,
          "esIndex" -> new SimpleDateFormat("YYYY.MM.dd").format(timestamp)
        )
      }
      )
  }

  def computeStatusCount(records:DStream[(Date,Int,String,Double,String,String)]):DStream[Map[String,Any]]={
    records
      .map(t=>((t._1,t._2,t._3,t._5,t._6),t._4))
      .groupByKey()
      .map(t =>
      {
        Map(
          "@timestamp" -> t._1._1,
          "status" -> t._1._2,
          "idc" -> t._1._3,
          "apiDomain" -> t._1._4,
          "apiName" -> t._1._5,
          "count" -> t._2.toList.length,
          "esIndex" -> new SimpleDateFormat("YYYY.MM.dd").format(t._1._1)
        )
      }
      )
  }

  def actionByDirect(args: Array[String]): Unit = {

    //1、参数获取
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }
    val Array(brokers, topics) = args

    //2、创建SSC
    val conf = new SparkConf()
    conf.setAppName("sinawap_nginx_belog_log_compute")
    conf.set("es.nodes", "10.211.103.202,10.211.103.212,10.211.103.222")
    //conf.setMaster("local[4]")
    //conf.setMaster("spark://10.211.103.201:7077")
    //conf.setJars(List("D:\\Clouds\\Baidu\\工作\\新浪\\Projects\\IDEA\\sina\\sina-logging\\wap-logging\\sinawap_nginx_belog\\target\\original-sinawap_nginx_belog-1.0-SNAPSHOT.jar"))
    //conf.setJars(List("/Volumes/Data/Projects/IDEA/sina/logging/cms_front_nginx/target/artifacts/CMSFrontNginx.jar"))
    //conf.set("spark.executor.memory", "2g")
    //conf.set("spark.executor.cores", "4")
    val ssc = new StreamingContext(conf, Seconds(5))


    //3、从kafka获取数据
    val topicsSet = topics.split(",").toSet
    val kafkaParams: Map[String, String] =
      Map[String, String](
        "metadata.broker.list" -> brokers,
        "group.id" -> "spark_receiver_cms_front_nginx_test"
      )
    val records = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    //4、执行计算

    //al result1 = computeStatusCount(cleanRecords(records))
    val result2 = computeRequestTime(cleanRecords(records))

    //5、保存结果

    //result1.saveToEs("spark_cms_front_nginx_status-{esIndex}/logs")
    //result2.saveToEs("spark_cms_front_nginx_request-{esIndex}/logs")
    result2.print()


    ssc.start()
    ssc.awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    actionByDirect(Array("10.71.216.62:9092,10.71.216.40:9092,10.71.216.47:9092,10.71.216.228:9092,10.71.216.229:9092", "sinawap-nginx-belog"))
  }
}
