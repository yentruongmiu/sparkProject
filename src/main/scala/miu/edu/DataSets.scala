package miu.edu

import org.apache.spark.sql.SparkSession

case class Cars(car: String, mpg: String, cyl: String, disp: String, hp: String,
                drat: String, wt: String, qsec: String, vs: String, am: String,
                gear: String, carb: String)
case class  Person(name: String, age: Int)

object DataSets {
  val FILENAME = "mtcars.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataSets")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext.setLogLevel("WARN")

    //Step 1: Read data from csv file
    import spark.implicits._

    val dataframe = spark.read.option("header", true).csv(FILENAME).cache()
    dataframe.printSchema()
    dataframe.show()


    //Step 2: Select a categoricalvariable and a numeric variale and from

    // Step 3: Compute the mean mpg and variance for each category


    // Step 4: Create the sample for bootstrapping. All you need to do is take 25% of the population without replacement.

    // Step 5: Do 1000 times

    // Step 6: Divide each quantity by 1000 to get the average and display the result.

  }
}
