package miu.edu

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Data: Gapminder data
 * continent: lifeExp
 */
object Main {
  val FILENAME = "gapminder.csv"
  val DATA_URL_PREFIX = "https://vincentarelbundock.github.io/Rdatasets/csv/causaldata"
  val FRACTION = 0.25
  val TIMES = 1000
  val COLUMNS: Array[String] = Array("continent", "lifeExp")
  val MIN_KEYS = 4

  def doubleRound(d: Double): Double = {
    val res = BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    res
  }

  def meanAndVarianceCalculation(rdd: RDD[(String, String)]): RDD[(String, (Double, Double))] = {
    val mean = rdd.mapValues(v => (v.toDouble, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(v => (v._1 / v._2, v._2))
      .sortByKey()

    val variance = rdd.mapValues(v => v.toDouble)
      .join(mean)
      .map(v => (v._1, (Math.pow(v._2._1 - v._2._2._1, 2), v._2._2._2)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2))
      .mapValues(v => {
        if (v._2 <= 1) {
          0
        } else {
          v._1 / (v._2 - 1)
        }
      })
      .sortByKey()

    val meanAndVariance = mean.mapValues(v => v._1).join(variance)
    meanAndVariance
  }

  def errorPercentage(actual: Double, estimate: Double): Double = {
    val result = Math.abs(actual - estimate) * 100 / actual

    doubleRound(result)
  }

  def downloadFile(url: String, file: String): Unit = {
    try {
      val src = scala.io.Source.fromURL("%s/%s".format(url, file))
      val out = new java.io.FileWriter(file)
      out.write(src.mkString)
      out.close()
    } catch {
      case e: java.io.IOException => println(e.getMessage)
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BigData - Spark Project")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._

    println("===== STEP 1 =====")
    println("Download dataset, read data from csv file")

    var dsFile = FILENAME
    if (args.length > 0) {
      dsFile = args(0)
    }

    downloadFile(DATA_URL_PREFIX, dsFile)
    val dataframe = spark.read.option("header", value = true).csv(FILENAME).cache()
    dataframe.printSchema()
    //dataframe.show()

    println("===== STEP 2 =====")
    println("Select a categorical variable and a numeric variable and form the key-value pair and create a pairRDD called \"population\"")

    val population = dataframe.select(COLUMNS(0), COLUMNS(1))
      .map(line => (line.getString(0), line.getString(1)))
      .cache()
    val rdd = population.rdd

    println("===== STEP 3 =====")
    println("Compute the mean mpg and variance for each category and display them")

    //population.toDF(COLUMNS(0), COLUMNS(1)).show()
    val meanAndVariance = meanAndVarianceCalculation(rdd)
    meanAndVariance.foreach(println)
    meanAndVariance.sortByKey().map(x => (x._1, doubleRound(x._2._1), doubleRound(x._2._2)))
      .toDF("Category", "Mean", "Variance")
      .show()

    println("===== STEP 4 =====")
    println("Create the sample for bootstrapping. All you need to do is take 25% of the population without replacement")

    //var bootstrapSample = sc.emptyRDD[(String, String)]
    var bootstrapSample = rdd.sample(withReplacement = false, FRACTION)
    bootstrapSample.foreach(println)

    println("===== STEP 5 =====")
    println("Do Step 4 1000 times")

    var sum: List[RDD[(String, (Double, Double))]] = List()
    var bootstrapSampleSize = bootstrapSample.count().toInt

    for (_ <- 1 to TIMES) {
      //5a. Create a “resampledData”. All you need to do is take 100% of the sample with replacement.
      val resampleData = bootstrapSample.takeSample(withReplacement = true, bootstrapSampleSize)

      //5b. Compute the mean mpg and variance for each category (similar to Step 3)
      val rdd = sc.makeRDD(resampleData)
      val resampleMeanAndVariance = meanAndVarianceCalculation(rdd)
      resampleMeanAndVariance.count()

      //5c. Keep adding the values in some running sum.
      sum = sum ::: List(resampleMeanAndVariance)
    }

    val sumUnion = sc.union(sum).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    println("===== STEP 6.A =====")
    println("Divide each quantity by 1000 to get the average and display the result")

    val average = sumUnion.map(v => (v._1, (v._2._1 / TIMES, v._2._2 / TIMES)))
      .sortByKey()

    average.map(v => (v._1, doubleRound(v._2._1), doubleRound(v._2._2)))
      .toDF("Category", "Mean", "Variance")
      .show()

    println("===== STEP 6.B =====")
    println("Determine the absolute error percentage for each of the values being estimated: " +
      "abs(actual – estimate)*100/actual")

    val dfAll = meanAndVariance.sortByKey().join(average)
      .map(x => (x._1, errorPercentage(x._2._1._1, x._2._2._1), errorPercentage(x._2._1._2, x._2._2._2)))
      .sortBy(x => x._1, ascending = true)
      .toDF("Continent", "Mean", "Variance")

    println("Percents")
    dfAll.show()

    //Step 7: Draw a graph with x-axis percentage
  }
}
