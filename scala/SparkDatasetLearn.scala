// Dataset[T]是Spark未来的弹性分布式数据集

package learn

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkDatasetLearn {

  def wordCountByDataset(session: SparkSession): Unit = {
    import session.implicits._
    val texts: Dataset[String] = session.read.textFile("./src/test/scala/wordCount.txt")
    val splitted: Dataset[String] = texts.filter(_!="").flatMap(_.split(" "))
    splitted.createOrReplaceTempView("splittedWords")
    val dataframe: DataFrame = session.sql("SELECT value AS word, count(*) AS freq FROM splittedWords GROUP BY value ORDER BY freq DESC")
    dataframe.show()
  }

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().
      appName("wordCountByDataset").master("local[4]").
      getOrCreate()
    wordCountByDataset(session)
    session.stop()
  }

}
