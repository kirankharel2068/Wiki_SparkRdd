package com.github.spark_wiki.tutorial

case class WikipediaArticle(title: String, text: String) {
  /**
   * @return whether the text of this article mentions 'lang' or not
   * @param lang language to look for
   */
def mentionsLanguage(lang: String):Boolean = text.split(" ").contains(lang)
}
