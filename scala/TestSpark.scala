package learn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestSpark {  // 一个只有object的scala文件可以当作脚本执行, 或者甚至可以不写object直接写语句
  // test spark libraries
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test-spark").setMaster("local[4]")  // configuration
    val sc: SparkContext = new SparkContext(conf)  // initialize
    sc.setLogLevel("WARN")  // minimize verbosity
    val data: Array[Double] = Array(1.0, 2.0, 3.0, 4.0, 5.0)
    val rdd: RDD[Double] = sc.parallelize(data)  // make RDD(only contains double)
    // RDD can only stores one certain type of data
    System.out.println(rdd.count())  // get length of the RDD
  }
}
