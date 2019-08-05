import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("SimpleApp").setMaster("spark://master1.0phsxb3scd5ezokhxq2qtfyv5e.syx.internal.cloudapp.net:7077")
    val sc = new SparkContext(conf)
    // val spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    val filePath ="/home/minji/spark-2.2.0-bin-hadoop2.7/README.md"
    val inputRDD = sc.textFile(filePath)
    val numAs = inputRDD.filter(line => line.contains("a")).count()
    val numBs = inputRDD.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
 //   System.exit()
   // spark.stop()
  }
}