# -*- coding: UTF-8 -*-

from pyspark.sql import SparkSession


def main():
    # 初始化一个Spark进程
    spark = SparkSession.builder.appName("test-pyspark-wordCount").master("local[4]").getOrCreate()
    # 将文本文件读入RDD
    rdd = spark.sparkContext.textFile("./test_text.txt")
    word_count = rdd.filter(
        lambda line: True if line != "" else False  # 去掉空白行
    ).flatMap(
        lambda line: line.strip().split(" ")  # 每行用空格分割后展平
    ).map(
        lambda word: (word.lower(), 1)  # 将每个词映射为(词 -> 1次)
    ).reduceByKey(
        lambda v1, v2: v1 + v2  # reducer: 对相同的词进行词计数
    ).sortBy(
        lambda kv: kv[1], ascending=False  # 结果按值(频数)降序排列
    )
    for kv in word_count.collect():
        print(kv)
    spark.stop()


if __name__ == '__main__':
    main()
