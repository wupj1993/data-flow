package com.wupj.bd.spark.fenci

import java.io.File
import java.nio.file.Paths
import java.util.Properties
import java.util.regex.Pattern

import com.huaban.analysis.jieba.{JiebaSegmenter, WordDictionary}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.util.matching.Regex

/**
  * 结巴分词测试
  */
object JiebaTest {
  def main(args: Array[String]): Unit = {
    songFenCi
  }

  def songFenCi(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val props = getMysqlProperties()
    val data = spark.read.format("jdbc")
      .options(props.asScala).load()
    data.createOrReplaceTempView("song_comments")
    import spark.implicits._
   val result= spark.sqlContext.sql("select CONTENT from song_comments limit 10").map(line => filterData(line.getString(0))).flatMap(line=>{
      fenci(line)
    }).map(line=>(line,1)).rdd.reduceByKey((_+_)).sortBy(_._2,false).collect().mkString(",\n")
    println(result)
  }

  def filterData(ori: String): String = {
    val regEx = "[ _`~!@#$%^&*()+=|{}':;',\\[\\].·<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]|\n|\r|\t"
    val p = Pattern.compile(regEx)
    val m = p.matcher(ori)
    val find = m.find
    val res = m.replaceAll("").trim
    val str = res.replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", "")
    str
  }
  def fenci(word:String): Seq[String] ={
    import com.huaban.analysis.jieba.JiebaSegmenter
    val segmenter = new JiebaSegmenter
    val list = segmenter.sentenceProcess(word)
    list.asScala.filter(line=>line.size>1)
  }
  def test(): Unit = {
    import com.huaban.analysis.jieba.JiebaSegmenter
    import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
    val segmenter = new JiebaSegmenter
    val sentences = Array[String](
      "这是一个伸手不见五指的黑夜。我叫孙悟空，我爱北京，我爱Python和C++。",
      "我不喜欢日本和服。",
      "雷猴回归人间。",
      "工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作",
      "结果婚的和尚未结过婚的")
    //    for (sentence <- sentences) {
    //      System.out.println(segmenter.process(sentence, SegMode.INDEX).toString)
    //    }
    val shige = "曾经￥沧海,难为水，除却巫山不是云"
    val pattern = new Regex("^a-zA-Z0-9\u4e00-\u9fa5");
    val result = pattern replaceAllIn(shige, "Java")
    println(result)
    val strings = segmenter.sentenceProcess(shige).asScala
    // 词典路径为Resource/dicts/jieba.dict
    /*    val path = Paths.get(new File(getClass.getClassLoader.getResource("dicts/jieba.dict").getPath).getAbsolutePath)
        WordDictionary.getInstance().loadUserDict(path)*/

    strings.foreach(data => println(data))
  }

  //  曾经沧海:1
  //  是:1
  //  水:1
  //  难:1
  //  巫山:1
  //  不:1
  //  为:1
  //  云:1
  //  除却:1
  // index
  /**
    * [[这是, 0, 2], [一个, 2, 4], [伸手, 4, 6], [不见, 6, 8], [五指, 8, 10], [伸手不见五指, 4, 10], [的, 10, 11], [黑夜, 11, 13], [。, 13, 14], [我, 14, 15], [叫, 15, 16], [悟空, 17, 19], [孙悟空, 16, 19], [，, 19, 20], [我, 20, 21], [爱, 21, 22], [北京, 22, 24], [，, 24, 25], [我, 25, 26], [爱, 26, 27], [python, 27, 33], [和, 33, 34], [c++, 34, 37], [。, 37, 38]]
    * [[我, 0, 1], [不, 1, 2], [喜欢, 2, 4], [日本, 4, 6], [和服, 6, 8], [。, 8, 9]]
    * [[雷猴, 0, 2], [回归, 2, 4], [人间, 4, 6], [。, 6, 7]]
    * [[工信处, 0, 3], [干事, 4, 6], [女干事, 3, 6], [每月, 6, 8], [经过, 8, 10], [下属, 10, 12], [科室, 12, 14], [都, 14, 15], [要, 15, 16], [亲口, 16, 18], [交代, 18, 20], [24, 20, 22], [口, 22, 23], [交换, 23, 25], [换机, 24, 26], [交换机, 23, 26], [等, 26, 27], [技术, 27, 29], [技术性, 27, 30], [器件, 30, 32], [的, 32, 33], [安装, 33, 35], [工作, 35, 37]]
    * [[结果, 0, 2], [婚, 2, 3], [的, 3, 4], [和, 4, 5], [尚未, 5, 7], [结过, 7, 9], [结过婚, 7, 10], [的, 10, 11]]
    *
    **/


  // search
  /*    [[这是, 0, 2], [一个, 2, 4], [伸手不见五指, 4, 10], [的, 10, 11], [黑夜, 11, 13], [。, 13, 14], [我, 14, 15], [叫, 15, 16], [孙悟空, 16, 19], [，, 19, 20], [我, 20, 21], [爱, 21, 22], [北京, 22, 24], [，, 24, 25], [我, 25, 26], [爱, 26, 27], [python, 27, 33], [和, 33, 34], [c++, 34, 37], [。, 37, 38]]
    [[我, 0, 1], [不, 1, 2], [喜欢, 2, 4], [日本, 4, 6], [和服, 6, 8], [。, 8, 9]]
    [[雷猴, 0, 2], [回归, 2, 4], [人间, 4, 6], [。, 6, 7]]
    [[工信处, 0, 3], [女干事, 3, 6], [每月, 6, 8], [经过, 8, 10], [下属, 10, 12], [科室, 12, 14], [都, 14, 15], [要, 15, 16], [亲口, 16, 18], [交代, 18, 20], [24, 20, 22], [口, 22, 23], [交换机, 23, 26], [等, 26, 27], [技术性, 27, 30], [器件, 30, 32], [的, 32, 33], [安装, 33, 35], [工作, 35, 37]]
    [[结果, 0, 2], [婚, 2, 3], [的, 3, 4], [和, 4, 5], [尚未, 5, 7], [结过婚, 7, 10], [的, 10, 11]]*/
  def getMysqlProperties(): Properties = {
    val jdbcHostname = "127.0.0.1"
    val jdbcPort = 3306
    val jdbcDatabase = "wymusic"
    val user = "root"
    val password = "757671834"
    val table = "song_comments"
    val driver = "com.mysql.jdbc.Driver"
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?useSSL=false"
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${user}")
    connectionProperties.put("password", s"${password}")
    connectionProperties.put("jdbcHostname", s"${jdbcHostname}")
    connectionProperties.put("port", s"${jdbcPort}")
    connectionProperties.put("url", s"${jdbcUrl}")
    connectionProperties.put("dbtable", s"${table}")
    connectionProperties.put("driver", s"${driver}")
    return connectionProperties;
  }


}
