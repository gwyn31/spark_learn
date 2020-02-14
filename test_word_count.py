# -*- coding: UTF-8 -*-

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("test-pyspark-wordCount").master("local[4]").getOrCreate()
    rdd = spark.sparkContext.textFile("./test_text.txt")
    word_count = rdd.filter(
        lambda line: True if line != "" else False
    ).flatMap(
        lambda line: line.strip().split(" ")
    ).map(
        lambda word: (word.lower(), 1)
    ).reduceByKey(
        lambda v1, v2: v1 + v2
    ).sortBy(
        lambda kv: kv[1], ascending=False
    )
    for kv in word_count.collect():
        print(kv)
    spark.stop()


if __name__ == '__main__':
    main()
