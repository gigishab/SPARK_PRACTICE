import org.apache.spark.SparkContext

val sc = new SparkContext("local[*]", "newwordcount")

val rdd1 = sc.textFile("C:\\spak\\sprkword.txt.txt")

val rdd2 = rdd1.flatMap(x => x.split(" ")).map(x => x.toLowerCase()).map(x => (x, 1)).reduceByKey((x, y) => x +y)

val res = rdd2.collect
for (r <- res) {
  val word = r._1
  val count = r._2

  println(s"$word : $count")

}