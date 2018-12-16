/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package ca.uwaterloo.cs451.project

import io.bespin.scala.util.Tokenizer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils
import scala.collection.mutable.ListBuffer

import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.sql.{SparkSession, SQLContext, SaveMode}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.ml._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover, HashingTF, IDF}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.PipelineModel
/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: NetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999`
 */
object TestTwitter extends Tokenizer{
  def main(args: Array[String]) {
    // if (args.length < 2) {
    //   System.err.println("Usage: NetworkWordCount <hostname> <port>")
    //   System.exit(1)
    // }



        val config = new SparkConf().setAppName("TestTwitter")
        val sc = new SparkContext(config)
        val ssc = new StreamingContext(sc, Seconds(5))
        val startTime: Long = System.currentTimeMillis/1000
        val nonEngRegex = "^[\\x00-\\x7F]+$".r
        val usernameRegex = "@[a-zA-Z0-9_]+".r

        val batchListener = new StreamingContextBatchCompletionListener(ssc, 2)
        ssc.addStreamingListener(batchListener)
        //ssc.setLogLevel("WARN")

        val stringify = udf((vs: Seq[String]) => vs match {
          case null => null
          case _    => s"""[${vs.mkString(",")}]"""
        })

         
        System.setProperty("twitter4j.oauth.consumerKey", "QgwDTkyT8XwXAzMJKsjQgrIXP")
        System.setProperty("twitter4j.oauth.consumerSecret", "gB6lfJpufaUoCYpWDrGj8lLOtxbQmTO79BEXQCgqx8pRX7BEaQ")
        System.setProperty("twitter4j.oauth.accessToken", "885045994429325312-tTZILxhh3k7sDErQpRaSXyZ5LAh6uXp")
        System.setProperty("twitter4j.oauth.accessTokenSecret", "QzbBtxO9C49jBhMdaQGJ3x6HZCrutAYL3UP7Fo9QOk55A")
         
        val stream = TwitterUtils.createStream(ssc, None, Seq(), StorageLevel.MEMORY_AND_DISK_2)
        val sqlContext= new org.apache.spark.sql.SQLContext(sc)



        val tweets = stream.map(status => (status.getText))
        .window(Seconds(100),Seconds(100))
        .filter(k => (nonEngRegex.pattern.matcher(k).matches))
        .map(line => (line,usernameRegex.replaceAllIn(line, "")))
        .map({case (lineO, line) => {
            val tokens = tokenize(line)
            // if (tokens.length > 0) tokens.take(tokens.length).sliding(tokens.length).toList else List()
            (lineO,tokens)
          }})
          .map({ case (lineO,line) => (lineO,line.take(line.length-1).filter(x => !((x.startsWith("http")) || (x.startsWith("rt") || (x.startsWith("@"))))).mkString(" "), -1.0D)})
        // .saveAsTextFiles("here")
        .foreachRDD{
            rdd => 
            // rdd.coalesce(1).saveAsTextFile("here")
            import sqlContext.implicits._
            val lrModel = PipelineModel.read.load("lr-model")
            val wordsDataFrame = rdd.toDF("original","text", "label")
            val resultsDF = lrModel.transform(wordsDataFrame)
            resultsDF.withColumn("filtered_new", stringify($"filtered")).select("original","text","filtered_new","prediction").coalesce(1).write.mode(SaveMode.Append).format("csv").save("here")
            // resultsDF.show()
        }    

        ssc.start()
        batchListener.waitUntilCompleted(() =>
          ssc.stop(true,true)
        )
    }
}

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
// scalastyle:on println