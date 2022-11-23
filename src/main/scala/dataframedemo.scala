
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.{SparkConf, SparkContext}

  object dataframedemo extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "anything")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    val orderDF = spark.read.option("header", true)
      //.option("inferSchema", true) // not recommended in production
                   .schema(ordersDDL)
                   .csv(path = "C:/Demos/input/orders.csv")

    orderDF.printSchema()

    orderDF.show()

    spark.stop()


    //scala.io.StdIn.readLine()

    //}
  }