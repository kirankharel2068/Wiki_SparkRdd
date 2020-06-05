package com.github.spark_wiki.tutorial

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait WikipediaRankingInterface {
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])]
  def occurencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int
  def rankLangs(lang: List[String], rdd: RDD[WikipediaArticle]): List[((String, Int))]
  def rankLangsReduceByKey(lang: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)]
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)]
  def langs: List[String]
  def sc: SparkContext
  def wikiRdd: RDD[WikipediaArticle]
}
