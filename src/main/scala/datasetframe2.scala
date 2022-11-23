  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.{SaveMode, SparkSession}
  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}


  class datasetframe2 {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "shab")
    sparkconf.set("spark.master", "local[*]")

    val sc = new SparkContext("local[*]", "Something")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    // val ordsersddl = "orderid Int, orderdate String, custid Int, orderstatus String" // order matters not name - this way u can make things into a int and string not all strings
    //spark.read.option("header",true).option("inferSchema",true).csv("C:\\Users\\agoni\\Downloads/orders.csv").show() // = this makes everything into string preferred not used.
    val orderschema = StructType(List(
      StructField("orderid", IntegerType, true),
      StructField("orderdate", TimestampType, true),
      StructField("custid", IntegerType, true),
      StructField("orderstat", StringType, true))) // this is made by spark and is more customizable
    val orderdf = spark.read.option("header", true)
      .schema(orderschema)
      .csv("C:\\Users\\agoni\\Downloads/orders.csv")

    // orderdf.show()
    // "orderid Int, orderdate String, custid Int, orderstatus String" could be used instead of orderschema

    val processedDF = orderdf.where("custid>10000").select("orderid", "orderstat", "custid") // narrow trans
    val pdf1 = processedDF.where("orderstat='COMPLETE'").select("orderstat", "orderid", "custid").groupBy("custid").count() // to look for strings do a double quote - wide transform


    pdf1.show()

    processedDF.write.format("csv").mode(SaveMode.Overwrite).option("path", "C:\\spak\\sprkword.txt.txt").save()



    spark.stop()
  }

}
