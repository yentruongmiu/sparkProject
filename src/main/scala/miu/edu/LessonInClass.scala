package miu.edu

import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}

object LessonInClass {
  def addInt(a: Int, b: Int): Int = {
    var sum: Int = 0
    sum = a + b
    sum
  }

  def IntMultiply(a: Int = 5, b: Int = 7): Int = a * b

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Study Spark and Scalal").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("===myOne")
    var myOne = List(List("cat", "mat", "bat"),
      List("hat","mat","rat"),
      List("cat","mat","sat"),
      List("cat","fat","bat"))
    myOne.foreach(println) // 4 lists

    println("===myTwo")
    var myTwo = sc.parallelize(myOne)
    myTwo.foreach(println) // 4 lists

    println("===myThree")
    var myThree = myTwo.map(x => (x(1), x(2)))
    myThree.foreach(println) // (mat, bat) \n (mat, rat)\n (mat, sat)\n (fat, bat)
    //println(myThree.collect().mkString(","))

    println("===myFour")
    var myFour = myThree.sortBy(_._1, false).sortBy(_._2)
    myFour.foreach(println) // (mat,bat),(fat,bat),(mat,rat),(mat,sat)

    val data1 = Array(7,8,2,10,4,10,9,4)
    val rdd1 = sc.parallelize(data1)
    val max1 = rdd1.max() //10
    val rdd2 = rdd1.filter(_ != max1) //Array(7,8,2,4,9,4)
    val max2 = rdd2.max() //9
    println("max2 is " + max2) //9

//    def parseLine(line:String): (String, Int) ={
//      val fields = line.split(",")
//      val name = fields(1).toString
//      val age = fields(2).toInt
//      (name, age)
//    }
//    val lines = sc.textFile("person.csv")
//    val rdd = lines.map(parseLine)
//    val res1 = rdd.mapValues(x => (x,1))

    //rdd.groupByKey().foreach(println)
//    res1.collect().foreach(println)
//    println("====")
//
//    val res2 = res1.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
//    res2.collect().foreach(println)
//    println("====")
//
//    val res3 = res2.mapValues(x => x._1/x._2)
//    res3.collect().sorted.foreach(println)

//    val f = sc.textFile("text.txt")
//    val words = f.flatMap(x => x.split(" ")).map(word => (word,1))
//    words.reduceByKey(_+_).collect().foreach(println)

//    val list = sc.parallelize(List(("abc", 2), ("def", 3), ("xyz", 10), ("dcm", 2), ("abc", 3)))
//    val countKey = list.countByKey()
//    countKey.foreach(println) // (dcm, 1) (abc, 2) (def, 1) (xyz, 1)
//
//    val countValue = list.countByValue()
//    countValue.foreach(println) //((xyz, 10), 1) ((def, 3), 1) ((abc, 3), 1) ((abc,2), 1) ((dcm,2), 1)

//    val numbers = sc.parallelize(List(7,8,9,10,12))
//    println(numbers.take(1).mkString("")) // return an array with first n element => use mkString("")
//    println(numbers.count())
//    println(numbers.first())

//    val numbers = sc.parallelize(List(7,8,9,10,12)).map(x => if(x%2 == 0) 1 else -1).reduce(_+_)
//    println(numbers) //1: event more than odd

//    val numbers = sc.parallelize(List(7,2,9))
//    val sum = numbers.reduce((x,y) => x + y)
//    println(sum) //18
//    val squareSum = numbers.map(x => x*x).reduce((x,y) => x+y)
//    println(squareSum) //134

//    val rdd1 = sc.parallelize(List(("Mech", 30), ("Mech", 40), ("Elec", 50)))
//    val valmapped = rdd1.mapValues(x => (x, 1)) //("Mech", (30, 1)) ("Mech", (40,1)) ("Elec", (50, 1))
//    val reduced = valmapped.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
//      reduced.foreach(println) //("Mech", (70, 2)) ("Elec", (50,1))

//    val average = reduced.map(x => {
//      val temp = x._2
//      val total = temp._1
//      val count = temp._2
//      (x._1, total / count)
//    })
//    average.foreach(println)

    //val one = sc.parallelize(List(1,2,1,4))
    //one.map(x => (x, 1)).groupByKey().foreach(println) //(4,Seq(1)) (1, Seq(1,1)) (2, Seq(1))

    //one.map(x => (x, 1)).sortBy(v => v._1, false).foreach(println) // (4,1) (2,1) (1,1) (1,1)
    //one.map(x => (x, 1)).sortByKey().foreach(println) // default: (1,1) (1,1) (2,1) (4,1)

    //val one = sc.parallelize(List(1,2,3,4))
    //val two = sc.parallelize(List(6,5,4,3))
//    val one_union_two = one.union(two)
    //println(one_union_two.distinct().collect().mkString(","))

    //println(one.intersection(two).collect().mkString(","))
    //println(two.subtract(one).collect().mkString(","))
    //println(one.cartesian(two).collect().mkString(","))



//    val one = sc.parallelize(List(1,2,3,4))
//    val three = one.sample(false, 0.5)
//    println(three.collect().mkString(","))

//    val two = one.filter(x => x > 2)
//    println(two.collect().mkString(","))
//    two.foreach(println)

//    var numbers = sc.parallelize(List(7,2,9))
//    var number1 = numbers.map(x => x*x)
//    println(number1.collect().mkString(","))
//    var numberList = numbers.flatMap(x => List(x*x, x*x*x))
//    println(numberList.collect().mkString(","))

//    val line = sc.parallelize(List("One upon a time", "Spark came"))
//    val words = line.flatMap(w => w.split(" "))
//    words.foreach(println)

    //    val sum = addInt(5, 7)
    //    println("5 + 7 = " + sum)

    //    val multiply = IntMultiply(10)
    //    println("10 * 7 = " + multiply)

    //    val myOne = List(
    //      List("cat", "mat", "bat"),
    //      List("hat", "mat", "rat"),
    //      List("cat", "mat", "sat"),
    //      List("cat", "fat", "bat"),
    //      List("eat", "fat", "cat"),
    //      List("hat", "fat", "pat")
    //    )
    //    myOne.foreach(println)
    //    println("==================")
    //
    //    val myTwo = sc.parallelize(myOne)
    //    myTwo.foreach(println)
    //    println("==================")
    //
    //    val myThree = myTwo.map(x => (x(1), x(2)))
    //    myThree.foreach(println)
    //    println("==================")
    //
    //    val myFour = myThree.sortBy(_._1, false).sortBy(_._2, true)
    //    myFour.foreach(println)
    //

//    val writer = new PrintWriter(new File("result.txt"))
//    writer.write("Hello World")
//
//    val tf = sc.textFile("text.txt")
//    val countOne = tf.flatMap(line => line.split("\\s+"))
//      .map(x => (x.toLowerCase(), 1))
//      .reduceByKey(_ + _)
//      .cache()
//    println("==================")
//    val countTwo = countOne.sortByKey(true)
//    countTwo.foreach(println)
//
//    val res = countTwo.collect()
//    for (n <- res) writer.println(n.toString())
//    writer.close()

//    val data1 = Array(7, 8, 2, 10, 4, 10, 9, 4, 1, 6)
//    val rdd1 = sc.parallelize(data1)
//    val max1 = rdd1.max()
//    val rdd2 = rdd1.filter(_ != max1)
//    val max2 = rdd2.max()

//    println(max1 + "=====" + max2)

//    def getCount(in: Array[Int], start: Int, stop: Int): Int = {
//      val cnt = in.map(x => if (x >= start && x <= stop)
//        1
//      else
//        0
//      ).reduce(_ + _)
//      cnt
//    }
//
//    println("\nNumber of items between 4 and 7 is " + getCount(data1, 4, 7))
//    println("\nSecond largest value is " + max2)
//
//    def isNumeric(input: String): Boolean = input.forall(_.isDigit)
//
//    val accessLog = sc.textFile("access_log").cache
//    val logRecord = accessLog
//      .filter(line => (line.split("\\s+").size > 9 && isNumeric(line.split("\\s+")(9))))
//      .map(line => (line.split("\\s+")(0), line.split("\\s+")(9)))
//      .filter(x => x._2 != "0").cache()
//
//    def getIP(in: String): String = {
//      in.split(" ")(0)
//    }
//
//    def getSite(in: String): String = {
//      in.split(" ")(6)
//    }
//
//    def getSize(in: String): String = {
//      val x = in.split(" ")
//      if (x.count(_ => true) > 9)
//        x(9)
//      else
//        "0"
//    }
//
//    val data = accessLog
//    val cleanData = data.map(x => (getIP(x), getSize(x)))
//      .filter(x => x._2 != "-")
//      .filter(x => x._2 != "")
//      .filter(x => x._2 != "0").cache()
//    val cleanDataInt = cleanData.map(x => (x._1, x._2.toInt))
//
//    println("====Print 10 item of cleanDataInt")
//    cleanDataInt.sortBy(_._1).top(10).foreach(println)
//
//    val cleanDataIntGroup = cleanDataInt.groupByKey()
//    println("====Print 10 item of cleanDataIntGroup")
//    cleanDataIntGroup.sortBy(_._1).top(10).foreach(println)
//
//    val cleanDataIntGroupMap = cleanDataIntGroup
//      .map(x => (x._1, (x._2.count(_ => true), x._2.reduce(_ + _))))
//    println("=====Print 10 item of cleanDataIntGroupMap")
//    cleanDataIntGroupMap.sortBy(_._1).top(10).foreach(println)
//
//    val average = cleanDataIntGroupMap.map(x => (x._1, x._2._2 / x._2._1))
//    println("\n\n\tPRINTING AVERAGES (just 10 rows)")
//    average.sortBy(_._1).top(10).foreach(println)

  }
}
