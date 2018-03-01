import org.apache.spark.{SparkConf, SparkContext}

object Ass17_2_2 extends App {

  val conf = new SparkConf().setAppName("Spark").setMaster("local")
  //Reading file from the specified path
  val spark = new SparkContext(conf).textFile("17.2_Dataset.txt")
  val arrayTuples = spark.map(line => line.split(",")).map(array => (array(0), array(1), array(2), array(3), array(4)))
  //1.count of students per grade in the school
  val countOne = arrayTuples.map(x=>(x._3,1)).reduceByKey(_+_).foreach(println)
  //2.Find the average of each student (Note - Mathew is grade-1, is different from Mathew in  some other grade!)
  val avgOfStudent = arrayTuples
    .map(x=>((x._1,x._3),(x._4.toFloat))).groupByKey().mapValues(lines=>(lines.sum/lines.size)).foreach(println)
  //3.What is the average score of students in each subject across all grades?
  val x = arrayTuples.map(x=>(x._2,x._4.toFloat)).groupByKey().mapValues(lines=>(lines.sum/lines.size)).foreach(println)
  //4.What is the average score of students in each subject per grade?
  val a = arrayTuples.map(x=>((x._2,x._3),x._4.toFloat)).groupByKey().mapValues(lines=>(lines.sum/lines.size)).foreach(println)
  //5.For all students in grade-2, how many have average score greater than 50?
  val scores = arrayTuples
    .map(x=>((x._1,x._3),(x._4.toFloat))).groupByKey().mapValues(lines=>(lines.sum/lines.size))
    .map{case(x,y)=> if(x._2.contains("grade-2") &&(y > 50)){println(x,y)}}.collect()

}
