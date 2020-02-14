package learn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestRDDTextFile {

  def wordCount(): Unit = {
    val filepath: String = "./src/test/scala/wordCount.txt"
    val conf: SparkConf = new SparkConf()  // configure
    conf.setMaster("local[2]").setAppName("wordCount")
    val spark: SparkContext = new SparkContext(conf)
    spark.setLogLevel("WARN")
    // 读取目标文件
    val text = spark.textFile(filepath)

    // word count(通过MapReduce)
    // MapReduce其实返回的也是键值对
    val splited: RDD[String] = text.flatMap(row => row.split(" "))  // 空格分分割得到单词, 即一个个单词的sequence
    // result example: RDD[String] = ["hello", "java", "hello", "spark", "hello", "scala"]
    val wordsMap: RDD[(String, Int)] = splited.filter(_ != "").map(word => (word, 1))  // 映射: word(key) -> 1(value, 即出现次数)
    // map process: 每个词出现, 计数一次, key为词, 初始value为1
    // result example(key -> value):
    // RDD[(String, Int)] = [("hello", 1), ("java", 1), ("hello", 1), ("spark", 1), ("hello", 1), ("scala", 1)]
    val wordCount: RDD[(String, Int)] = wordsMap.reduceByKey(
      (value1: Int, value2: Int) => {value1 + value2}
    )  // 将拥有同样key的记录相加
    // reduce process(即reduceByKey): 匿名函数(x, y) => x + y的原理:
    /**
     * 1. 对有相同key的记录分组:
     * [("hello", 1), ("hello", 1), ("hello", 1)], [("java", 1)], [("spark", 1)], [("scala", 1)]
     * 2. 对相同key的记录的values执行(x, y) => x + y的逻辑:
     * 对于第一组(key为hello): 第一次执行得到: [("hello", 2) // x, ("hello", 1) // y]; 第二次执行得到[("hello", 3)], 只剩一条记录, 返回结果
     * 注: 对于只有一条记录的组如[("java", 1)], 该匿名函数不会执行, 直接返回结果
     **/
    wordCount.sortBy(kv => kv._2, ascending = false). // 按词频降序排列, 注意元组元素的访问语法: tuple._2(表示元组的第二个元素)
      foreach(println)

    // 以上操作的简写:
    /**
     * val wordCountSimple: RDD[(String, Int)] = text.flatMap(_.split(" ")).
     *  filter(_ != "").map((_, 1)).
     *  reduceByKey(_ + _)
     *
     * wordCountSimple.foreach(println)
     **/

    val wordCountSimple: RDD[(String, Int)] = text.flatMap(_.split(" ")).filter(_ != "").map((_, 1)).reduceByKey(_ + _)

    spark.stop()
  }

  def printArray(strArr: Array[String]): Unit = {
    // print a string array
    val s: StringBuilder = new StringBuilder()
    s.append("[")
    for (i <- 0 to strArr.length-2) {
      s.append(strArr(i) + ", ")
    }
    s.append(strArr(strArr.length-1)).append("]")
    System.out.println(s.toString())
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("test-spark-text-file")
    val sc: SparkContext = new SparkContext(conf)
    val testTextFile: RDD[String] = sc.textFile("./src/test/scala/bank-full.csv")

    // transformation
    // val splitted = testTextFile.map(row => row.split(';'))  // 以分号分割(注意scala匿名函数的写法: [input arg] => [output arg])
    val splitted = testTextFile.map(_.split(';'))  // 上面这一行的匿名函数的简写形式: 用_代表row, 单下划线在scala中有特别的意义
    // action
    val length = splitted.count()
    val head = splitted.take(2)
    System.out.println(length)
    head.foreach(printArray)
    sc.stop()
  }
}
