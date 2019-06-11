package com.wupj.bd.spark.fenci

import org.ansj.recognition.impl.StopRecognition

/**
  * 基于Ansj的分词
  */
object AnsJWordSplitTest {
  def main(args: Array[String]): Unit = {

  }

  /**
    * 设置停用词
    *
    * @param stopWords
    * @return
    */
  def filter(stopWords: Array[String]): StopRecognition = {
    val stopWord = new StopRecognition

    stopWord.insertStopWords()
    stopWord
  }
}
