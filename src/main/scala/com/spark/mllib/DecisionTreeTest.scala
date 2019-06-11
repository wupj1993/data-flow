package com.spark.mllib

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 决策树
  * 决策树学习：根据数据的属性采用树状结构建立决策模型。决策树模型常常用来解决分类和回归问题。
  * 常见的算法包括 CART (Classification And Regression Tree)、ID3、C4.5、随机森林 (Random Forest) 等。
  *
  */
object DecisionTreeTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("DecisionTreeTest").getOrCreate()
    val data = MLUtils.loadLibSVMFile(spark.sparkContext, "E:\\javaworkspace\\yyt-data-flow\\data-flow\\src\\main\\resources\\sample_libsvm_data.txt")
    val array = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (array(0), array(1))
    val numClasses = 2 // 分类数量
    val categoricalFeaturesInfo = Map[Int, Int]() // 用map存储类别（离散）特征及每个类别特征对应值（类别）的数量
    val impurity = "gini" // 纯度计算方法
    val maxDepth = 5 // 树的最大高度 建议值5
    val maxBins = 32 // 用于分裂特征的最大划分数量 建议值32
    val model: DecisionTreeModel = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    // 误差计算
    val labelAndPreds: RDD[(Double, Double)] = testData.map { point =>
      val prediction: Double = model.predict(point.features)
      (point.label, prediction)
    }
    val print_predict: Array[(Double, Double)] = labelAndPreds.take(20)
    println("label" + "\t" + "prediction")
    for (i <- 0 to print_predict.length - 1)
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("错误率:"+testErr)
    println("learned classification tree model:\n" + model.toDebugString)
    // 保存模型
    //    val modelPath = "modelPath/DecisionTreeModel"
    //    model.save(spark.sparkContext, modelPath)
  }
}
