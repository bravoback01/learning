package com.ddos.processor

import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.text.SimpleDateFormat
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec
import java.util.Date
import java.time.format.DateTimeFormatter
import java.util.Calendar
import scala.collection.mutable.ListBuffer
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import scala.collection.immutable.Map
import scala.collection.immutable.TreeMap
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import com.ddos.log.parser.AccessLogParser

/**
 * This code is used to detect DDOS attack.
 */
object Driver2 {

  def main(args: Array[String]): Unit = {

    val group_id = args(0)
    val topic_id = args(1)
    
    //val conf = new SparkConf().setMaster("local[10]").setAppName("Simple Streaming Application")
    val conf = new SparkConf().setAppName("Simple Streaming Application")
    val ssc = new StreamingContext(conf, Seconds(2))
    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
    import sqlContext.implicits._
    
    ssc.checkpoint("/tmp/stremcheckpoint")

    val topicsSet = Set(topic_id)
    
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> "largest")

    /**
     * Defining specification for mapwithstate function.
     * Added timeout. State of any IP is removed if not access in last 20 seconds.
     */
    val stateSpec = StateSpec.function(updateFunctionwithKey _).timeout(Seconds(20))

    val directKafkaStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topicsSet, kafkaParams))

    val refMap = ssc.sparkContext.broadcast(loadAccessThersholdRefData)
    
    directKafkaStream.map(r => parseConsumer(r.value))
      .filter(_._1.size > 0)
      .combineByKey(createCombiner, mergeValue, mergeCombiner, new HashPartitioner(10), true)
      .mapWithState(stateSpec).map(checkDeviation(_,refMap.value))
      .filter(!_.isEmpty)
      .repartition(1)
      .foreachRDD(rdd => {
        if(!rdd.isEmpty()) {
          rdd.toDF().write.mode(SaveMode.Append).text("/tmp/ddos")
        }  
      })
      
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Check deviation based on reference data and IP access stats.
   * 
   */
  def checkDeviation(obj: Option[AccessTimeCount], refMap:Map[Int,Int]): Option[String] = {

    var out = ""
    
    obj match {
      case Some(objAccessTimeCnt) => {
        val objAccessTimeCnt = obj.get
       
        refMap.foreach(map => {
          val rangeinsec = map._1
          val accesslimit = map._2
          val accesswindow = objAccessTimeCnt.dtCnt.takeRight(rangeinsec)
          val accesscnt = accesswindow.values.sum
          if (accesscnt >= accesslimit) {
            out = out + objAccessTimeCnt.key + "," + accesswindow.keySet.toString() + ","+ accesscnt + "," + accesslimit + "\n"
          }
        })
        
      if(out.length() > 0)
        Some(out)
       else
         None
      }
     case None => None
    }
  }

  /**
   * Parse input records and return IP addess and access date time.
   * In case of invalid records, returns empty string.
   */
  val parser = new AccessLogParser
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def parseConsumer(cs: String): (String, String) = {
    val record = parser.parseRecord(cs)
    record match {
      case Some(rec) => (rec.clientIpAddress, sdf.format(AccessLogParser.parseDateField(rec.dateTime).get))
      case None      => ("", "")
    }
  }

  
  /**
   * Combiner function to count access per IP per sec. 
   * Map side reduction is on.
   */
  def createCombiner = (dttime: String) => {
    Map[String, Int](dttime -> 1)
  }

  def mergeValue = (map: Map[String, Int], dttime: String) => {
    map + (dttime -> (map.getOrElse(dttime, 0) + 1))
  }

  def mergeCombiner = (map: Map[String, Int], map1: Map[String, Int]) => {
    (map.keySet ++ map1.keySet).map { i => (i, map.getOrElse(i, 0) + map1.getOrElse(i, 0))}.toMap
    
  }

  
  /**
   * Spark mapwithstate funtion: This function is used to hold state of access count per IP per second. 
   * Given real time streaming nature, latest 20 records is more than sufficient to evaluate attack.
   */
  def updateFunctionwithKey(key: String, value: Option[Map[String, Int]], state: State[Map[String, Int]]): Option[AccessTimeCount] = {

    val statemap = state.getOption.getOrElse(Map.empty[String, Int])
    value match {
      case Some(inputmap) => {
        val merge = (inputmap.keySet ++ statemap.keySet).map { i => (i, inputmap.getOrElse(i, 0) + statemap.getOrElse(i, 0)) }.toMap

        val sortedmap = TreeMap(merge.toSeq: _*)
    
        state.update(sortedmap.takeRight(20))
    
        Some(AccessTimeCount(key, sortedmap.takeRight(20)))
      }
      case None => None
      
    }
  }
  
  /**
   * Load reference data. This data include time window and access limit per IP. 
   * For e.g. More than 25 hit in 5 secs is considered as attack.
   * TODO: Load data from HDFS or Hive table. Need to refresh after specified time
   * 
   */
  def loadAccessThersholdRefData:Map[Int,Int] = {
    Map((5 -> 25), (10 -> 45))
  }

}








