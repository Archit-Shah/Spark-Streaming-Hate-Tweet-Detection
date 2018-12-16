/**
  *
  * University of Waterloo
  * Course: CS 651 - Data Intensive Distributed Computing
  * Author: Archit Shah (20773927)
  * Final Project: Hate/Abusive Tweet Detection with Spark Streaming
  *
  * BuildPipeline - This file performs following functions:
  * 1. Prepare & cleanse training dataset
  * 2. Define functions to tokenise, transform (extract features), train and predict
  * 3. Build the pipeline with above functions
  *
  **/

package ca.uwaterloo.cs451.project

import io.bespin.scala.util.Tokenizer
import java.io._
import scala.io.Source
import math._
import org.rogach.scallop._
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover, HashingTF, IDF}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.lit


//Configuration class to recieve command line inputs
class ConfBuildPipeline(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(inputPath, modelPath)
  val inputPath = opt[String](descr = "Input Path", required = true)
  val modelPath = opt[String](descr = "Model Path", required = true)
  // val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object BuildPipeline extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  
  def main(argv: Array[String]) {
  	val args = new ConfBuildPipeline(argv)

    log.info("Input Path: " + args.inputPath())
    log.info("Model Path: " + args.modelPath())

	//Spark Configuration
  	val conf = new SparkConf().setAppName("BuildPipeline")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val ModelDir = new Path(args.modelPath())
    FileSystem.get(sc.hadoopConfiguration).delete(ModelDir, true)

    val newColumns = Seq("text", "type", "label")
    val textFile = sc.textFile(args.inputPath() + "/hatespeech_text_label_vote.csv")

    val Array(training, test) = textFile.randomSplit(Array(0.9, 0.1), seed = 12345)

	//Extract training data from the text file, filter usernames, hyperlinks and Retweet identifier. Convert the RDD to a Dataframe
    val trainingData = training
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 0) tokens.take(tokens.length).sliding(tokens.length).toList else List()
      })
      .map(line => line.take(line.length-1).filter(x => !((x.startsWith("http")) || (x.startsWith("rt") || (x.startsWith("@"))))).mkString(" ") -> line.last)
      .map({case (k,v) => (k,v,if (v=="abusive" || v=="hateful") 1.0D else 0.0D)})
	  .toDF(newColumns: _*)

    val testingData = test
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 0) tokens.take(tokens.length).sliding(tokens.length).toList else List()
      })
      .map(line => line.take(line.length-1).filter(x => !((x.startsWith("http")) || (x.startsWith("rt") || (x.startsWith("@"))))).mkString(" ") -> line.last)
      .map({case (k,v) => (k,v,if (v=="abusive" || v=="hateful") 1.0D else 0.0D)})
	  .toDF(newColumns: _*)

	//Pipeline Definition with 5 stages

	// 1. Tokenizer to convert sentences into an array of words, delimited by space character
	val custom_tokenizer = new RegexTokenizer()
	  .setInputCol("text")
	  .setOutputCol("words")

	// 2a. Stopword definition retrieved from an external file
	val stopwords: Array[String] = sc.textFile(args.inputPath() + "/stopwords.txt").collect

	// 2b. Define a filter function to remove stopwords
	val data_filter = new StopWordsRemover()
	  .setStopWords(stopwords)
	  .setCaseSensitive(false)
	  .setInputCol("words")
	  .setOutputCol("filtered")

	// 3. Extract raw feature of Term Frequency with HashingTF
	val hashingTF = new HashingTF()
	  .setInputCol("filtered").setOutputCol("termFreq")

	// 4. Extract final features with Inverse Document Frequency. Set the limit of minimum document frequency to minimize false positives
	val idf = new IDF()
	.setMinDocFreq(5)
	.setInputCol("termFreq")
	.setOutputCol("features")

	// 5. Define the training algorithm
	val lr = new LogisticRegression()
	  .setMaxIter(10)
	  // .setRegParam(0.2)
	  // .setElasticNetParam(0.0)



	//Initialise the the pipeline with pre-defined stages
	val pipeline = new Pipeline().setStages(Array(custom_tokenizer, data_filter, hashingTF, idf, lr))


	val paramGrid = new ParamGridBuilder()
	  .addGrid(hashingTF.numFeatures, Array(10, 100, 1000, 10000))
	  .addGrid(lr.regParam, Array(0.1, 0.01, 0.001))
	  .addGrid(lr.elasticNetParam, Array(0.0, 0.1, 0.01))
	  .build()

	val cv = new CrossValidator()
	  .setEstimator(pipeline)
	  .setEvaluator(new RegressionEvaluator)
	  .setEstimatorParamMaps(paramGrid)
	  .setNumFolds(3)  // Use 3+ in practice
	  // .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

	//Fit the cleansed trainingData into the LogisticRegression model defined via pipeline
	val lrModel = cv.fit(trainingData)

	//Save the trained model into the Model Path, so it can be used later to predict
	lrModel.write.save(args.modelPath() + "/lr-model")


	//Predict on the Testing data and display the overall accuracy of the model
	val predictDF = lrModel.transform(testingData)
	val results = predictDF.withColumn("final_results", when($"prediction" === $"label",1).otherwise(0)).select("final_results")
	val incorrect = results.rdd.map(v => v.getAs[Int](0)).filter(v => v==0)
	val correct = results.rdd.map(v => v.getAs[Int](0)).filter(v => v==1)

	var c = correct.count()
	var i = incorrect.count()
	var accuracy = (c.toFloat/(c+i))*100

	printf("The accuracy of model is %f",accuracy)

  }}