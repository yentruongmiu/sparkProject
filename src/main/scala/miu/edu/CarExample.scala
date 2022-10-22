package miu.edu

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class Cars(car: String, mpg: String, cyl: String, disp: String, hp: String,
                drat: String, wt: String, qsec: String, vs: String, am: String,
                gear: String, carb: String)

case class Person(name: String, age: Int)

object CarExample {
  val FILENAME = "mtcars.csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Example about Cars").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    //Step 1: Read data from csv file
    import spark.implicits._
//    val dataframe = spark.read.option("header", true).csv(FILENAME).cache()
//    dataframe.printSchema()
//    dataframe.show();
    val csv = spark.sparkContext.textFile(FILENAME)

    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val mtdata = headerAndRows.filter(_(0) != header(0))
    val mtcars = mtdata.map(p => Cars(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11)))
      .toDF()
    mtcars.printSchema()
    mtcars.select("mpg").show(5)
    mtcars.filter(mtcars("mpg") < 18).show()

    import org.apache.spark.sql.functions._
    mtcars.groupBy("cyl").agg(avg("wt")).show()
    mtcars.groupBy("cyl").agg(count("wt")).sort($"count(wt)".desc).show()

    mtcars.withColumn("wtTon", mtcars("wt") * 0.45).select(("car"), ("wt"), ("wtTon")).show(6)

    val mtcarsDS = mtdata.map(p => Cars(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11)))
      .toDS

    mtcarsDS.printSchema()
    mtcarsDS.select("mpg").show(5)
    mtcarsDS.filter($"mpg" < 18).show()

    mtcarsDS.groupBy("cyl").agg(avg("wt")).show()
    mtcarsDS.groupBy("cyl").agg(count("wt")).sort($"count(wt)".desc).show()
    mtcarsDS.select($"wt"*0.45 as "wt45", $"cyl" as "Cylinder").show(6)


    val info = List(("mike", 24), ("joe", 34), ("jack", 55))
    val infoRDD = spark.sparkContext.parallelize(info)
    val people = infoRDD.map(r => Person(r._1, r._2)).toDS()
    people.show()

    val peopleAsTuple = infoRDD.map(r =>(r._1, r._2)).toDS()
    peopleAsTuple.show()

    spark.stop()
    //Step 2: Select a categorical variable and a numeric variale and from

    // Step 3: Compute the mean mpg and variance for each category


    // Step 4: Create the sample for bootstrapping. All you need to do is take 25% of the population without replacement.

    // Step 5: Do 1000 times

    // Step 6: Divide each quantity by 1000 to get the average and display the result.

  }
}
