package miu.edu

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition.first
import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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
    val res = BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
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

  def errorPercentage(actual:Double, estimate:Double): Double = {
    val result = Math.abs(actual - estimate)*100/actual
    //println("actual:"+actual + ", estimate:"+estimate + ", result:"+result)
    doubleRound(result)
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

    //Determine the absolute error percentage for each of the values being estimated: abs(actual – estimate)*100/actual
    val dfAll = meanAndVariance.sortByKey().join(average)
      .map(x => (x._1, errorPercentage(x._2._1._1, x._2._2._1), errorPercentage(x._2._1._2, x._2._2._2)))
      .sortBy(x => x._1, true)
      .toDF("Continent", "Mean", "Variance") //"Continent", "Mean", "Variance"
      //.show()
    println("======PERCENTS======")
    //percentsAll.foreach(println)
    dfAll.show()


//    val res = dfAll.withColumn("Continent", monotonicallyIncreasingId).groupBy("Continent").pivot("Mean")
//      .agg(dfAll.first()).show()

    //val newSchema = StructType(dfAll.select(0).first().getAs[Seq[String]](0).map(z => StructField(z, StringType)))
//    val new_schema = StructType(df1.select(collect_list("Column")).first().getAs[Seq[String]](0).map(z => StructField(z, StringType)))
//    val new_values = sc.parallelize(Seq(Row.fromSeq(df.select(collect_list("Value")).first().getAs[Seq[String]](0))))
//    sqlContext.createDataFrame(new_values, new_schema).show(false)

      //(Africa, (2.01, 12.13))
      //(Asia, (0.60, 8.62))
    //(Americas, (0.51, 8.04))
    //(Oceania, (0.98, 11.12))
    //(Europe, (0.51, 17.17))
    //=> (Percentage, Africa-M, Africa-V, Asia-M, Asia-V, Americas-M, Americas-V, Oceania-M, Oceania-V, Europe-M, Europe-V)
    //=> (25,  (2.01, 12.13), (0.60, 8.62), (0.51, 8.04), (0.98, 11.12), (0.51, 17.17)


//    (Africa,48.86533012820508,49.065439087995095,83.72634723273455,100.6580062781063)
//    (Asia,60.064903232323175,61.199140705446766,140.76710837747206,131.42413997450512)
//    (Americas,64.65873666666667,64.52482573703185,87.33066977992195,85.08237005427381)
//    (Oceania,74.32620833333333,73.10676174324293,14.406663302536224,16.540266747112838)
//    (Europe,71.90368611111106,71.05560947014297,29.519421096185418,45.3717385773788)

    //Step 7: Draw a graph with x-axis percentage
  }
}
