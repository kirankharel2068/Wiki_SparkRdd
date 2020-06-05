package com.github.spark_wiki.tutorial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  //setup spark, we need conf to create spark context which is the door to all spark functionalities
  val conf = new SparkConf().setAppName("Wikipedia").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    //1. read in wikipedia data
    //a. wikipediaData.filepath returns filepath as string after getting it from resource
    // after that sc.textfile reads file and passes each line to parse which returns Wikipedia Article type RDD
    val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(WikipediaData.parse)

    //Rank languages attempt #1: rankLangs
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))
    println("### Output of part1 ### "+langsRanked)

    //Rank languages attempt #2: rankLangsUsingIndex
    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)
    val langsRanked2: List[(String, Int)] = timed("Part2: ranking using inverted index",
      rankingLangsUsingIndex(index))
    println("### Output of Part 2 ###"+langsRanked2)

    /* Rank languages attempt #3: rankLangsReduceByKey */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

  }
  // Implementing the helper classes
  def occurencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int =
    rdd.filter(_.mentionsLanguage(lang))
      .aggregate(0)((x,y) => x+1, _+_)

  def rankLangs(lang: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
    lang.map(x => (x, occurencesOfLang(x, rdd))).sortBy(v => v._2).reverse

  //Implementing the helper classes for part 2

  /* Compute an inverted index of the set of articles, mapping each language
 * to the Wikipedia pages in which it occurs.
 */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] =
    rdd.flatMap(article => langs.filter(l => article.mentionsLanguage(l)).
    map(item => (item, article))).groupByKey()

  def rankingLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] =
    index.mapValues(v => v.size).sortBy(-_._2).collect().toList

  //part 3
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
    rdd.flatMap(article => langs.filter(article.mentionsLanguage).map((_,1))).reduceByKey(_+_).sortBy(-_._2).collect().toList


  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
