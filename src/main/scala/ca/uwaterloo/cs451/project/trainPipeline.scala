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
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover, HashingTF, IDF}
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression

object trainPipeline extends Tokenizer {
  def main(args: Array[String]) {

  	val conf = new SparkConf().setAppName("TrainTwitter")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val textFile = sc.textFile("data/hatespeech_text_label_vote.csv")
    val newColumns = Seq("text", "type", "label")

    val trainingData = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 0) tokens.take(tokens.length).sliding(tokens.length).toList else List()
      })
      .map(line => line.take(line.length-1).filter(x => !((x.startsWith("http")) || (x.startsWith("rt") || (x.startsWith("@"))))).mkString(" ") -> line.last)
      .map({case (k,v) => (k,v,if (v=="abusive" || v=="hateful") 1.0D else 0.0D)})
	  .toDF(newColumns: _*)

	val custom_tokenizer = new RegexTokenizer()
	  // .setGaps(false)
	  // .setPattern("\\p{L}+")
	  .setInputCol("text")
	  .setOutputCol("words")

	val stopwords: Array[String] = sc.textFile("data/stopwords.txt").collect

	val filterer = new StopWordsRemover()
	  .setStopWords(stopwords)
	  .setCaseSensitive(false)
	  .setInputCol("words")
	  .setOutputCol("filtered")

	// val countVectorizer = new CountVectorizer()
	//   .setInputCol("filtered")
	//   .setOutputCol("features")

	val hashingTF = new HashingTF()
	  .setInputCol("filtered").setOutputCol("rawFeatures")

	val idf = new IDF()
	.setMinDocFreq(5)
	.setInputCol("rawFeatures")
	.setOutputCol("features")


	val lr = new LogisticRegression()
	  .setMaxIter(10)
	  .setRegParam(0.2)
	  .setElasticNetParam(0.0)

	val pipeline = new Pipeline().setStages(Array(custom_tokenizer, filterer, hashingTF, idf, lr))

	val lrModel = pipeline.fit(trainingData)

	lrModel.write.save("lr-model")

	def toEmotion(pred: Double): String = if (pred == 1.0) "negative" else "positive"
	sqlContext.udf.register("toEmotion", toEmotion _)

	val newColumns1 = Seq("text", "label")
	case class Tweet(text: String, label: Double)
	val testDF1 = sc.parallelize(Seq(("test", -1.0D),("I don't like that dumb cunt", -1.0D)))
	import sqlContext.implicits._
	val testDF = testDF1.toDF(newColumns1: _*)
	
	val results = lrModel.transform(testDF)
	results.show()

  }
}