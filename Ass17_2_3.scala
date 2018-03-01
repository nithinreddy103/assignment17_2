import org.apache.spark.{SparkConf, SparkContext}

object Ass17_2_3 extends App {
  val conf = new SparkConf().setAppName("Spark").setMaster("local")
  //Reading file from the specified path
  val spark = new SparkContext(conf).textFile("17.2_Dataset.txt")
  val arrayTuples = spark.map(line => line.split(",")).map(array => (array(0), array(1), array(2), array(3), array(4)))
   val x = arrayTuples.map(x=>(x._1,x._4.toFloat)).groupByKey().mapValues(lines=>(lines.sum/lines.size))
  val a = arrayTuples.map(x=>((x._1,x._3),x._4.toFloat)).groupByKey().mapValues(lines=>(lines.sum/lines.size))
  val xxx = a.map(x=>(x._1._1,x._2))
  val finalResult = x.intersection(xxx).foreach(println)
}
