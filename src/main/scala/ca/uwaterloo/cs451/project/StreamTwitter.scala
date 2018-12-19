/**
  *
  * University of Waterloo
  * Course: CS 651 - Data Intensive Distributed Computing
  * Author: Archit Shah (20773927)
  * Final Project: Hate/Abusive Tweet Detection with Spark Streaming
  *
  * StreamTwitter - This file performs following functions:
  * 1. Stream Data from Twitter in real-time
  * 2. Transform and cleanse tweets with the Pipeline defined in BuildPipeline
  * 3. Classify the tweets as whether they are hateful/abusive or normal with the pre-defined Model
  *
  **/

package ca.uwaterloo.cs451.project

import io.bespin.scala.util.Tokenizer
import scala.collection.mutable.ListBuffer
import java.util.concurrent.atomic.AtomicInteger

import org.rogach.scallop._
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.sql.{SparkSession, SQLContext, SaveMode}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.ml._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover, HashingTF, IDF}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}

//Configuration class to recieve command line inputs
class ConfStreamTwitter(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(modelPath, outputPath, numBatch)
  val modelPath = opt[String](descr = "Model Path", required = true)
  val outputPath = opt[String](descr = "Output Path", required = true)
  val numBatch = opt[Int](descr = "Number of Batches", required = true)
  verify()
}

object StreamTwitter extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  
  def main(argv: Array[String]) {
    val args = new ConfStreamTwitter(argv)

    //Log info for command line parameters
    log.info("Model Path: " + args.modelPath())
    log.info("Output Path: " + args.outputPath())
    log.info("Number of Batches: " + args.numBatch())

    //Setting up Twitter Credentials. These Credentials will stop working after 31st December
    System.setProperty("twitter4j.oauth.consumerKey", "QgwDTkyT8XwXAzMJKsjQgrIXP")
    System.setProperty("twitter4j.oauth.consumerSecret", "gB6lfJpufaUoCYpWDrGj8lLOtxbQmTO79BEXQCgqx8pRX7BEaQ")
    System.setProperty("twitter4j.oauth.accessToken", "885045994429325312-tTZILxhh3k7sDErQpRaSXyZ5LAh6uXp")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "QzbBtxO9C49jBhMdaQGJ3x6HZCrutAYL3UP7Fo9QOk55A")

    //UDF to convert the Array Type column into a concatenated string
    val stringify = udf((vs: Seq[String]) => vs match {
      case null => null
      case _    => s"""[${vs.mkString(",")}]"""
    })

    val config = new SparkConf().setAppName("StreamTwitter")
    val sc = new SparkContext(config)
    val ssc = new StreamingContext(sc, Seconds(5))
    //Batch Listener to accpunt for number of batches to be processed
    val batchListener = new StreamingContextBatchCompletionListener(ssc, args.numBatch())
    ssc.addStreamingListener(batchListener)

    //Regex expressions to be used for data cleansing
    val nonEngRegex = "^[\\x00-\\x7F]+$".r
    val usernameRegex = "@[a-zA-Z0-9_]+".r
    //Delete output directory if already exists
    val OutputDir = new Path(args.outputPath())
    FileSystem.get(sc.hadoopConfiguration).delete(OutputDir, true)

    val stream = TwitterUtils.createStream(ssc, None, Seq(), StorageLevel.MEMORY_AND_DISK_2)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)

    //Twitter DStream with data cleansing and model prediction with the pre-trained model
    val tweets = stream.map(status => (status.getText))
      .window(Seconds(60),Seconds(60))
      .filter(k => (nonEngRegex.pattern.matcher(k).matches))
      .map(line => (line,usernameRegex.replaceAllIn(line, "")))
      .map({case (lineO, line) => {
          val tokens = tokenize(line)
          (lineO,tokens)
        }})
        //Removing hyperlinks, usernames and RT tags
        .map({ case (lineO,line) => (lineO,line.take(line.length-1).filter(x => !((x.startsWith("http")) || (x.startsWith("rt") || (x.startsWith("@"))))).mkString(" "), -1.0D)})
      .foreachRDD{
          rdd => 
          import sqlContext.implicits._
          val lrModel = CrossValidatorModel.read.load(args.modelPath())
          val wordsDataFrame = rdd.toDF("original","text", "label")
          val resultsDF = lrModel.transform(wordsDataFrame)
          resultsDF
            .withColumn("filtered_new", stringify($"filtered"))
            .withColumn("result", when($"prediction" === 1.0D,"Hateful/Abusive").otherwise("Normal"))
            .select("original","result")
            .coalesce(1).write.mode(SaveMode.Append).format("csv").save(args.outputPath())
      }    
    ssc.start()
    //Invoking an error to stop the DStream after a particular number of Batches. Else, the DStream will run indefinitely.
    batchListener.waitUntilCompleted(() =>
      ssc.stop(true,true)
    )
  }
}

//Batch Listener class to keep track of number of batches
class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
  def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
    while (!sparkExSeen) {}
    cleanUpFunc()
  }

  val numBatchesExecuted = new AtomicInteger(0)
  @volatile var sparkExSeen = false

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    val curNumBatches = numBatchesExecuted.incrementAndGet()
    if (curNumBatches == limit) {
      sparkExSeen = true
    }
  }
}

