package com.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}

object Ass17_2_1 extends App {

  val conf = new SparkConf().setAppName("Spark").setMaster("local")
  //Reading file from the specified path
  val spark = new SparkContext(conf).textFile("17.2_Dataset.txt")
  //1.Create a tupled RDD
  val arrayTuples = spark.map(line => line.split(",")).map(array => (array(0), array(1), array(2), array(3), array(4))).collect
  //2.count of total number of rows present.
  System.out.println("Count of total number of rows: "+arrayTuples.size)
  //3.distinct number of subjects present in the entire school
  val subjects = arrayTuples.map(x => x._2).distinct.size
  System.out.println("number of subjects present in the entire school: " +subjects)
  //4.count of the number of students in the school, whose name is Mathew and  marks is 55
  val countOne = arrayTuples.filter(x=> (x._1.equals("Mathew"))&&(x._4.equals("55"))).size
  System.out.println("count of the number of students in the school: " +countOne)
}
