package com.github.spark_wiki.debugging

import com.github.spark_wiki.tutorial.{WikipediaArticle, WikipediaData}
import com.github.spark_wiki.tutorial.WikipediaRanking.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Debugging {
  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  //setup spark, we need conf to create spark context which is the door to all spark functionalities
  val conf = new SparkConf().setAppName("Wikipedia").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(WikipediaData.parse)

    //lets see how part 2 is done step by step
    wikiRdd.collect().take(2).foreach(x =>println("====>"+x))
    wikiRdd.flatMap(_)
//    wikiRdd.flatMap(article => langs.filter(l => article.mentionsLanguage(l)).
//      map(item => (item, article))).groupByKey()
  }

}
