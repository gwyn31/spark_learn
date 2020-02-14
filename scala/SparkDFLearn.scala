package learn

import java.util.Properties

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkDFLearn {

  def testCSV(spark: SparkSession): Unit = {
    val filepath: String = "./src/test/scala/bank-full.csv"
    val df: DataFrame = spark.read.option("header", true).option("sep", ";").
      csv(filepath)  // 读取csv作为dataframe, read.option可以配置读取选项
    df.show(5)  // 查看前5行
    println(df.count())  // 数据集的行数
  }

  def testJDBC(spark: SparkSession): Unit = {
    val url: String = "jdbc:mysql://localhost:3306"  // 注意URL的写法: jdbc:[database]://[home]:[port]
    val connectionProperties: Properties = new Properties()  // 连接参数
    connectionProperties.put("user", "root")  // 用户
    connectionProperties.put("password", "z2026419")  // 密码
    val df: DataFrame = spark.read.option("driver", "com.mysql.cj.jdbc.Driver").
      jdbc(url, "test.spark_test", connectionProperties)
    // 注意要使用com.mysql.cj.jdbc.Driver(com.mysql.jdbc.Driver已弃用)
    df.createOrReplaceTempView("df")  // 在内存中注册该df为一个临时的view(类似于数据库中的临时表)
    df.show(10)
    println(df.count())
    // 可以对DataFrame执行SQL查询
    val result: DataFrame = spark.sql("SELECT * FROM df WHERE some_datetime <= '2019-06-02 02:03:05' ORDER BY some_datetime ASC")
    result.show(10)
    println(result.count())
  }

  def testETL(spark: SparkSession): Unit = {
    val filepath: String = "./src/test/scala/train.csv"  // 数据文件路径(kaggle的titanic数据集)
    val dtypeArray: Array[StructField] = Array(  // 定义每列的数据类型(schema) -> DataFrame的风格, 以列存储和访问数据
      StructField("PassengerId", IntegerType, nullable = false),
      StructField("Survived", IntegerType, nullable = false),
      StructField("Pclass", IntegerType, nullable = false),
      StructField("Name", StringType, nullable = false),
      StructField("Sex", StringType, nullable = true),
      StructField("Age", IntegerType, nullable = true),
      StructField("SibSp", IntegerType, nullable = false),
      StructField("Parch", IntegerType, nullable = false),
      StructField("Ticket", StringType, nullable = false),
      StructField("Fare", DoubleType, nullable = false),
      StructField("Cabin", StringType, nullable = true),
      StructField("Embarked", StringType, nullable = true)
    )
    val dtypes: StructType = new StructType(dtypeArray)  // 数据类型的schema
    // 也可以在options Map中设置"inferSchema" -> "true"来自动推断列类型
    val readOptions: Map[String, String] = Map("header" -> "true", "sep" -> ",")  // 文件读取的选项
    val df: DataFrame = spark.read.schema(dtypes). // 通过已有的schema指定表结构
      format("csv"). // 数据源类型
      options(readOptions). // 读取选项
      load(filepath)  // 加载文件
    df.show(10)  // 查看前10行数据
    // 统计男女的存活数和平均Fare
    val sexSurvived: DataFrame = df.filter("Survived = 1").
      groupBy("Sex").
      agg(Map("PassengerId" -> "count", "Fare" -> "avg"))
    sexSurvived.show()
    // 转化为RDD  // 这是一个object
    val dfRDD = df.rdd
    dfRDD.take(10).foreach(println)

  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().
      master("local[2]").appName("learn-DataFrame").getOrCreate()  // 初始化一个SparkSession(代替了旧的SQLContext)
    // SparkSession相当于一个console
    spark.conf.set("spark.executor.memory", "500M")
    testETL(spark)
    spark.stop()  // shutdown spark
  }
}
