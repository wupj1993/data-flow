package com.spark.mllib

import org.apache.spark.mllib.fpm.{FPGrowth}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * FP-growth 算法
  * 【支持度】指某频繁项集在整个数据集中的比例。
  * 假设数据集有 10 条记录，包含{'鸡蛋', '面包'}的有 5 条记录，
  * 那么{'鸡蛋', '面包'}的支持度就是 5/10 = 0.5。
  * 【置信度】是针对某个关联规则定义的。有关联规则如{'鸡蛋', '面包'} -> {'牛奶'}，它的置信度计算公式为{'鸡蛋', '面包', '牛奶'}的支持度/{'鸡蛋', '面包'}的支持度。假设{'鸡蛋', '面包', '牛奶'}的支持度为 0.45，{'鸡蛋', '面包'}的支持度为 0.5，则{'鸡蛋', '面包'} -> {'牛奶'}的置信度为 0.45 / 0.5 = 0.9。
  *
  **/
object FPGrowthDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FPGrowthDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rddData = sc.textFile("E:\\javaworkspace\\yyt-data-flow\\data-flow\\src\\main\\resources\\sample_fpgrowth.txt")
    val mapData = rddData.map(_.split(" "))
    val minSupport = 0.3 // 最小支持度
    val result = new FPGrowth().setMinSupport(minSupport)
      .setNumPartitions(10)
      .run(mapData)

    result.freqItemsets.collect().foreach(fp => {
      // 大于1才有算伴随的作用，我觉得吧
      if(fp.items.size>1){
        println(fp.items.mkString("[", ",", "]") + "," + fp.freq)
      }
    })
  }
}
