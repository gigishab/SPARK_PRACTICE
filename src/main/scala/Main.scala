import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}


object Main {
  def main (args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val sc = new SparkContext("local[*]", "newwordcount")
    //read from file
    val rdd1 = sc.textFile("C:\\spak\\sprkword.txt.txt")
    //using anonymous holder
    //one input row will give multiple output rows /val rdd2 = rdd1.flatMap(x => x.split(" "))
    val rdd2 = rdd1.flatMap(x => x.split(" "))
    //val rdd21 = rdd2.map(x => x.toLowerCase())
    //one input row will give one output row only
    val rdd3 = rdd2.map(x => (x, 1))
    //take two rows , and does aggregation and returns one row
    val rdd4 = rdd3.reduceByKey((x, y) => x + y)
    // print the output
    rdd4.collect.foreach(println)

    //val rdd2 = rdd1.flatmap(x => x.split("")).map(x => x.toLOwerCase()).map(x => (x, 1)).reduceByKey((x, y) => x +y)

  }
}