package com.wupj.bd.spark.streaming

import org.ansj.domain.Term
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 分词测试
  */
object AnsjTest {
  /**
    * 过滤暂停词
    *
    * @param stopWords
    * @return
    */
  def filter(stopWords: Array[String]): StopRecognition = {
    // add stop words
    val filter = new StopRecognition
    filter.insertStopNatures("w") // filter punctuation
    filter.insertStopNatures("m") // filter m pattern
    filter.insertStopNatures("null") // filter null
    filter.insertStopNatures("<br />") // filter <br />
    filter.insertStopRegexes("^[a-zA-Z]{1,}") //filter English alphabet
    filter.insertStopRegexes("^[0-9]+") //filter number
    filter.insertStopRegexes("[^a-zA-Z0-9\\u4e00-\\u9fa5]+")
    filter.insertStopRegexes("\t")
    for (x <- stopWords) {
      println("停用词:"+x)

      filter.insertStopWords(x)
    }
    filter
  }

  def getWords(text: String, filter: StopRecognition): ArrayBuffer[String] = {
    val words = new mutable.ArrayBuffer[String]()
    val terms: java.util.List[Term] = ToAnalysis.parse(text).recognition(filter).getTerms
    for (i <- 0 until terms.size()) {
      val word = terms.get(i).getName
    //  if (word.length >= 2) {
        words += word
      //}
    }
    words
  }

  def main(args: Array[String]): Unit = {
    val stopWords = new Array[String](1)
//    stopWords.update(0,"时间")

    val res = getWords("曾经沧海难为水，除却巫山不是云", filter(stopWords = stopWords))
    val sc = SparkSession.builder().appName("fenci").master("local[1]").getOrCreate()
    val tuples = sc.sparkContext.makeRDD(res).map((_,1)).reduceByKey(_+_).collect()
    for (x <- tuples) {
      println(x._1+":"+x._2)
    }
  }
}
