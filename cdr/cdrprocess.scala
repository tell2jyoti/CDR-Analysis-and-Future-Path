

/**
 * @author jyoti Ranjan Panda
 */
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.io.{FloatWritable, Writable, Text}
import org.apache.hadoop.hbase.client.{Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka._

object cdrprocess {
  def main(args: Array[String]) {
    
    def updateFunction(values: Seq[Float], runningCount: Option[Float]) = {
      val newCount = values.sum + runningCount.getOrElse(0.0f)
      new Some(newCount)
    }

    val sparkConf = new SparkConf().setAppName("Real Time Sales Ingestion").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
   // val hdfs = FileSystem.get(new java.net.URI("hdfs://localhost:8020"), hadoopConf)
    val hdfs = FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)

    // Create direct kafka stream with brokers and topics
    val brokers = "localhost:9092" 
    val topics = "cdr"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet) 
    val lines = messages.map(_._2) 
    
   // ssc.checkpoint("hdfs://localhost:8020/spark-demo/checkpoints")
    ssc.checkpoint("hdfs://localhost:9000/spark-demo/checkpoints")
    
    val cdrDataRDD = lines.map(row => {

      val columnValues = row.split(",")
      val cdrDate = columnValues(0).substring(0, columnValues(0).indexOf(" "))
      val carrierId = columnValues(2)
      val accountId = columnValues(1)
      val count = columnValues(3).toFloat
      ((cdrDate,carrierId,accountId),count)
    })
    
    val trafficCount = cdrDataRDD.reduceByKey(_ + _)
    val totalTrafficCount = trafficCount.updateStateByKey(updateFunction _)
    
    totalTrafficCount.foreachRDD ( rdd => {
                                       
      val conf = HBaseConfiguration.create()
      conf.set(TableOutputFormat.OUTPUT_TABLE, "cdrdata")
      conf.set("hbase.zookeeper.quorum","localhost:2222")
      conf.set("hbase.rootdir", "/usr/lib/hbase-0.96.2-hadoop2")
      val jobConf = new Configuration(conf)
      jobConf.set("mapreduce.job.output.key.class", classOf[Text].getName)
      jobConf.set("mapreduce.job.output.value.class", classOf[FloatWritable].getName)
      jobConf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Text]].getName)
      rdd.map(convert).saveAsNewAPIHadoopDataset(jobConf)
    })
        
    totalTrafficCount.map(a => a._1._1 + "," + a._1._2 + "," + a._1._3 + "," + a._2).print()
    ssc.start()
    ssc.awaitTermination()
  }
  
  def convert(t: ((String,String,String), Float)) = {
    val p = new Put(Bytes.toBytes(t._1._1 + "-" + t._1._2 + "-" + t._1._3 ))
    p.add(Bytes.toBytes("count"), Bytes.toBytes("tarfficcount"), (t._2).toString().getBytes())
    (t._1, p)
  }
}
