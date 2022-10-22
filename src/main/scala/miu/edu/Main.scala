package miu.edu

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Data: Gapminder data
 * continent: lifeExp
 */
object Main {
  val FILENAME = "gapminder.csv"
  val FRACTION = 0.25
  val TIMES = 1000
  val COLUMNS = Array("continent", "lifeExp")
  val MIN_KEYS = 4

  def doubleRound(d: Double): Double = {
    val res = BigDecimal(d).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    res
  }

  def meanAndVarianceCalculation(spark: SparkSession, rdd: RDD[(String, String)]): RDD[(String, (Double, Double))] = {
    val mean = rdd.mapValues(v => (v.toDouble, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(v => (v._1 / v._2, v._2))
      .sortByKey()

    //mean.foreach(println)
    val variance= rdd.mapValues(v => v.toDouble)
      .join(mean)
      .map(v => (v._1, (Math.pow(v._2._1 - v._2._2._1, 2), v._2._2._2)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2))
      .mapValues(v => {
        if(v._2 <= 1) {
          0
        } else {
          v._1 / (v._2 - 1)
        }
      })
      .sortByKey()
    //variance.foreach(println)

    val meanAndVariance = mean.mapValues(v => v._1).join(variance)
    meanAndVariance
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BigData - Spark Project")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    //Step 1: Download dataset, read data from csv file
    import spark.implicits._

    val dataframe = spark.read.option("header", true).csv(FILENAME).cache()
    dataframe.printSchema()
    //dataframe.show()

    //Step 2. Select a categorical variable and a numeric variable and form the key-value pair and create a pairRDD called “population”.
    val population = dataframe.select(COLUMNS(0), COLUMNS(1))
      .map(line => (line.getString(0), line.getString(1)))
      .cache()
    val rdd = population.rdd

    //Step 3: Compute the mean mpg and variance for each category and display them
    population.toDF(COLUMNS(0), COLUMNS(1)).show()
    val meanAndVariance = meanAndVarianceCalculation(spark, rdd)
    //meanAndVariance.foreach(println)
    meanAndVariance.sortByKey().map(x => (x._1, doubleRound(x._2._1), doubleRound(x._2._2)))
      .toDF("Category", "Mean", "Variance")
      .show()


    //Step 4: Create the sample for bootstrapping. All you need to do is take 25% of the population without replacement.

    var bootstrapSample = sc.emptyRDD[(String, String)]
    while(bootstrapSample.keys.distinct().count() < MIN_KEYS) {
      bootstrapSample = rdd.sample(false, FRACTION)
    }
    //println("==============")
    //bootstrapSample.foreach(println)


    //Step 5: Do 1000 times
    var sum: List[RDD[(String, (Double, Double))]] = List()
    for (i <- 1  to TIMES) {
      //5a. Create a “resampledData”. All you need to do is take 100% of the sample with replacement.
      val resampleData = bootstrapSample.sample(true, 1)

      //5b. Compute the mean mpg and variance for each category (similar to Step 3)
      val resampleMeanAndVariance = meanAndVarianceCalculation(spark, resampleData)
      resampleMeanAndVariance.count()
      //5c. Keep adding the values in some running sum.
      sum = sum:::List(resampleMeanAndVariance)
    }

    val sumUnion = sc.union(sum).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))


    //Step 6: Divide each quantity by 1000 to get the average and display the result
    val average = sumUnion.map(v => (v._1, (v._2._1 / TIMES, v._2._2 / TIMES)))
      .sortByKey()
    //average.foreach(println)
    average.map(v => (v._1, doubleRound(v._2._1), doubleRound(v._2._2)))
      .toDF("Category", "Mean", "Variance")
      .show()

    //Step 7: Draw a graph with x-axis percentage
  }
}
