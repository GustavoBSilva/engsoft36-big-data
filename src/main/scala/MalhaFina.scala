package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MalhaFina {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Malha Fina")

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Malha Fina")
      .getOrCreate()

    val datasetsPath = "/home/gustavo/Projetos/MalhaFina/datasets/"

    val rdd = sc.textFile(datasetsPath + "pessoas.csv")

    rdd.take(2).foreach(println)
  }
}