package ComPKG

import org.apache.spark._

import sys.process._
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
/*import org.apache.spark.SparkContext.jarOfObject
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import shapeless.syntax.std.tuple.unitTupleOps
import spire.implicits.eqOps*/

object ComplexDataObj {
  def main(args: Array[String]):Unit ={

      val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      val spark = SparkSession.builder().getOrCreate()

      println("===read the JSON file====")

      val df = spark.read.format("json").load("file:///D:/data/devices.json")
      df.show(3)

      println("====filterdf===")

      val filterDF = df.filter(col("lat")>70)
      filterDF.show(5)

    println("write the data in parquet format====")

      filterDF.write.format("parquet").mode("overwrite").save("file:///D:/data/Tasl_out/")

      println("====Task 2/read csv with delimiter ~ ====")

      val df1 = spark.read.format("csv").option("header","true").option("delimiter","~").load("file:///D:/data/txns_head")
      df1.show(2)

      val fetchMM = df1.selectExpr(
        "txnno",
        "split(txndate,'-')[1] as month",
        "custno",
        "amount",
        "category",
        "product",
        "city",
        "state",
        "spendby"
      )
      fetchMM.show(3)


      println("=================Domain Specific Language=============")

      println("=======Filter Operations in DSL======")

      val data = spark.read.format("csv").option("header","true").option("delimiter","~").load("file:///D:/data/txns_head")
      data.show(5)

      println("====1) Read txns_head with header true and filter category='Gymnastics'===")

      val filterdf = data.filter(col("category")==="Gymanstics")
      filterdf.show(5)

      println("====2) Read txns_head with header true and filter category='Gymnastics' and spendby='cash' -- Multi COlumn Filter===")

      val multifilterdf = data.filter(col("category")==="Gmanstics" && col("spendby") ==="cash")
      multifilterdf.show(4)

      println("====3) Filter category = 'Gymnastics' and category = 'Team Sports'  -- Multi Value Filter for the same Column ----- IN Operator===")

      val infilterdf = data.filter(col("category").isin("Gymnastics","Team Sports"))
      infilterdf.show(3)

      println("=====filter not category equals to Gymnastics=====")
      val notEqualDf = data.filter(col("category") === "Gymnastics")
      notEqualDf.show(3)

      println("====filter column Gymnastics and filter word Gymnastics from prodcut (It means contains)====")
      val filterContains = data.filter(!col("category")==="Gymnastics" && (col("product") like "%Gymnastics%"))
      filterContains.show(2)


      println("=======need define struct type while loading the data======")
    val columns = new StructType()
        .add("id", IntegerType)
        .add("name", StringType)
        .add("check", StringType)
        .add("spendby", StringType)
        .add("country", StringType)
      val data1 = spark.read.format("csv").option("header","true").load("file:///D:/data/usdata.csv")
      data.show()
      println("========data print in json format in partitioned columns=======")
      data1.write.format("json").partitionBy("county","state").mode("overwrite")
        .save("file:///D:/data/partitionedData/")


      println("========print xml ddataframe========")

      val xmldf = spark.read.format("com.databricks.spark.xml")
        .option("rowTag","POSLog")
        .load("file:///D:/data/transactions.xml")
      xmldf.show()
      xmldf.printSchema()

      println("===============Task 2 completed (DSL Operations)===========")

      val data2 = spark.read.format("csv")
        .option("header","true")
        .load("file:///D:/data/usdata.csv")
      val statedata1 = data2.filter("state='LA'")
      statedata1.show()
      val selectdf = statedata1.select("first_name", "last_name")
      selectdf.show()

      println("========spark xml read and write=======")
       
      val xmldf1 = spark.read.format("com.databricks.spark.xml")
        .option("rowTag","book")
        .load("file:///D:/data/book.xml")
      xmldf1.show()
      xmldf1.printSchema()

      println("========Spark RDs read and write=====")


      val sqldf = spark.read.format("jdbc")
        .option("driver","com.mysql.jdbc.Driver")
        .option("url","jdbc:mysql://zeyodb.chwk6wgpdls3.ap-south-1.rds.amazonaws.com:3306/zeyodata")
        .option("dbtable","cashdata")
        .option("user","root")
        .option("password","Aditya908")
        .load()

      sqldf.show()

    println("======JSON Read and Processing======")
      val jsondf = spark.read.format("json").load("file:///D:/data/devices.json")
      jsondf.show(5)

      jsondf.write.format("parquet").mode("overwrite").save("file:///D:/data/output_parquet/out")

      jsondf.write.format("csv").option("header","true").option("demiliter","~").save("file:///D:/data/output_csv/out")


      println("=======Task1 Completed========")

      val csvdf = spark.read.format("csv").option("header","true").load("file:///D:/data/usdata.csv")
      csvdf.show(5)

      //csvdf.write.format("parquet").mode("overwrite").save("file:///D:/data/Task1_output/")

      val parquetdf = spark.read.format("parquet").load("file:///D:/data/Task1_output/a.parquet")
      parquetdf.show(5)

      parquetdf.write.format("json").mode("overwrite").save("file:///D:/data/JSONLocation")

      val JSONReaddf = spark.read.format("json").load("file:///D:/data/JSONLocation/part-00000-ccd434a4-7c3e-473c-93b3-7d89d1dadb7e-c000.json")
      JSONReaddf.show(5)

      JSONReaddf.write.format("orc").mode("overwrite").save("file:///D:/data/ORCLocation/")

      println("=====Task2 completed======")

      val jsonreaddf = spark.read.format("json").load("file:///D:/data/random10.json")
      jsonreaddf.show(5)

      val jsondf1 = spark.read.format("json").option("multiline","true").load("file:///D:/data/random10.json")

      jsondf1.show(5)
      jsondf1.printSchema()

      println("===========Spark Avro Read and Write=========")

      val df2 = spark.read.format("avro").option("header","true").load("file:///D:/data/part_av.avro")
      df2.show(5)

      val df3 = spark.read.format("csv").option("header","true").load("file:///D:/data/usdata.csv")
      df3.show(5)

      println("======Create Temp View=====")

      df3.createOrReplaceTempView("df1")

      val df4 = spark.sql("select * from df1 where state ='LA'")
      df4.show()


      println("==========spark struct type=======")
      val data3 = sc.textFile("file:///D:/data/txnsd.txt")
      data3.foreach(println)
      val mapsplit = data3.map(x => x.split(","))
      val rowrdd = mapsplit.map(x => Row(x(0), x(1), x(2)))
      rowrdd.foreach(println)
      val filterrowrdd = rowrdd
        .filter(x => x(2).toString().contains("Gymnastics"))

      println("===========row rdd filter===============")
      filterrowrdd.foreach(println)
      val schemastruct = new StructType()
        .add("txnno", StringType)
        .add("txndate", IntegerType)
        .add("category", StringType)
      val df6 = spark.createDataFrame(filterrowrdd, schemastruct)

      df6.show()


  }

}
