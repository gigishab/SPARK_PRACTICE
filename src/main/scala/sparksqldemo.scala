import org.apache.spark. {SparkConf, SparkContext}
import org.apache.log4j. {Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


object sparksqldemo extends App {
  Logger.getLogger("org").setLevel (Level.ERROR)
  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "DFDemo")
  sparkconf.set("spark.master", "local [*]")
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val orderDF = spark.read.option ("header", true)
    .option("inferSchema", true) // not recommended in production
    .csv ("C:\\Users\\shabn\\OneDrive\\Desktop\\wordcount")


  orderDF.CreateOrReplaceTempView("tablenameorder")


  val res = spark.sql("select order_satus,count(*) as totOrders from tablenameOrder group by order_status")
  res.show()


