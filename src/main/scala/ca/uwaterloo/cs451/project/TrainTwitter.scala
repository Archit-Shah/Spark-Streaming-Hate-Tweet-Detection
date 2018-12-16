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

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import java.io._
import scala.io.Source
import math._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression


class TrainTwitter(args: Seq[String]) extends ScallopConf(args)  {
  // mainOptions = Seq(input, output, reducers)
  // val input = opt[String](descr = "input path", required = true)
  // val output = opt[String](descr = "output path", required = true)
  // val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  // val threshold = opt[Int](descr = "threshold", required = false, default = Some(1))
  // verify()
}

object TrainTwitter extends Tokenizer {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val conf = new SparkConf().setAppName("TrainTwitter")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val textFile = sc.textFile("data/hatespeech_text_label_vote.csv")

    val newColumns = Seq("text", "type", "label")

    val tweetsDF = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 0) tokens.take(tokens.length).sliding(tokens.length).toList else List()
      })
      .map(line => line.take(line.length-1).filter(!_.contains("http","1","2","3","4","5","6","7","8","9","0")) -> line.last)
      .map({case (k,v) => (k,v,if (v=="abusive" || v=="hateful") 1 else 0)})
      .toDF(newColumns: _*).show()
      
    
    // tweetsDF.createOrReplaceTempView("tweets")
    
    // val testQuery = sqlContext.sql("SELECT * FROM tweets")

    //   .map(v => v.distinct)
    //   .flatMap(v => v.sliding(1))
    //   .map(v => v(0) -> 1)
    //   .reduceByKey(_+_, varReducers)

    // val mgSingleCounts = sc.broadcast(mgSingleCountsB.collectAsMap())

    // val PMI = textFile
    //   .flatMap(line => {
    //     val tokens = tokenize(line)
    //     if (tokens.length > 1) tokens.take(min(tokens.length,40)).sliding(min(tokens.length,40)).toList else List()
    //   })
    //   .map(v => (v.distinct cross v.distinct))
    //   .map(v => v.filter(x => x != null))
    //   .flatMap(v => v.map(x => (x._1,x._2) -> 1))
    //   .reduceByKey(_+_,varReducers)
    //   .filter(v => v._2 >= varThreshold)
    //   .map(v => (v._1._1,v._1._2) -> ("%1.10f".format(log10((((v._2.toDouble/mgSingleCounts.value.getOrElse(v._1._1,1))/mgSingleCounts.value.getOrElse(v._1._2,1))*lnCounts.value.getOrElse("Count",1)))), v._2))

    // PMI.saveAsTextFile(args.output())

  }
}
