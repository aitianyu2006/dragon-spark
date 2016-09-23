package com.remarkmedia.spark.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, Word2Vec}
import org.apache.spark.sql.SQLContext

/**
  * Created by hz on 16/8/24.
  */
object WbClassifier {

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage:WbClassifier WbTextFile")
      sys.exit(1)
    }
    val VECTOR_SIZE = 100
    val conf = new SparkConf().setAppName("WEIBO MLPC Classification")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)
    val parsedRDD = sc.textFile(args(0)).map(_.split(",")).map(eachRow => {
      (eachRow(0), eachRow(1).split(" "))
    })
    val msgDF = sqlCtx.createDataFrame(parsedRDD).toDF("label", "message")
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(msgDF)

    val word2Vec = new Word2Vec()
      .setInputCol("message")
      .setOutputCol("features")
      .setVectorSize(VECTOR_SIZE)
      .setMinCount(1)

    val layers = Array[Int](VECTOR_SIZE, 6, 5, 2)
    val multilayerPerceptronClassifier = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(512)
      .setSeed(1234L)
      .setMaxIter(128)
      .setFeaturesCol("features")
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")



    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val Array(trainingData, testData) = msgDF.randomSplit(Array(0.8, 0.2))
    multilayerPerceptronClassifier.fit(trainingData)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec, multilayerPerceptronClassifier, labelConverter))
    val model = pipeline.fit(trainingData)

    val predictionResultDF = model.transform(testData)
    predictionResultDF.printSchema
    predictionResultDF.select("message", "label", "predictedLabel").show(30)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val predictionAccuracy = evaluator.evaluate(predictionResultDF)
    println("Testing Accuracy is %2.4f".format(predictionAccuracy * 100) + "%")
    sc.stop
  }
}
