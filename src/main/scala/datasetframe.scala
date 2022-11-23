
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

import java.sql.TmeTmp

//case class ordersdata(order_id:string, order_date:timestamp, order_customer:int, order_status:string)
case class orderdate(order_id:string, order_date:string, order_customer_id:int, order_status:string)
object dsdemo extend app {

  //def main(args: Array[string]): unit = {

  logger.getlogger("org").setlevel(level.error)

  val sparkconf = new sparkconf()
   sparkconf.set("spark.app.name", "dfdemo")
  sparkconf.set("spark.master", "Local[*]")

  val spark = sparksession.builder().config(sparkconf).getorcreate()

  // val ordersddl = "orderid int, order_date string, custid int, orderstatus string"
  StructureField("orderid", IntegerType, true),
  StructureField("orderdate", TimestampType, true),
  StructureField("custid", IntegerType, true),
  StructureField("orderstatus", StringType, true),
  ))

  val oderdf = spark.read.option("header", true).option("inferschema", true) // not recommended in production
  //.schema(ordersschema)
    .csv(path = "")

  import spark.implicits_
  val orderds = orderdf.as[ordersdata]

  val processeddf = orderdf.where("custid>10000").select("orderid", "orderstatus","custid") //narrow
    .groupby("custid").count() //wide transformation reduceby
  //
  //how to write outputdata in a file
 //orderdf.write.format("csv").mode(savemode.overwrite).option("path", "").save()


  //how to write output data in a table


processedDF.write.format(source = "orders.csv").mode(saveMode.Overwrite).saveAsTable(tableName= "order")

