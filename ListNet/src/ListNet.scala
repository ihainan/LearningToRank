/**
 * Created by ihainan on 1/22/15.
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Sorting
import collection.JavaConversions._


class ListNet(val ITERATIONS:Int, val STEP_SIZE:Double) extends Serializable{

  /* Classes */
  // A class used to present a QUERY
  class Query(val queryID:Int, val numberOfDocuments:Int, val realScores:Array[Int], val docFeatures:Array[Vector])
   extends Serializable{}

  /* Global Variables */
  def DIM = 137          // The number of features
  // def ITERATIONS = 10    // The number of iterations
  // def STEP_SIZE = math.pow(10.0, -4)
  def DEBUG = true

  var model:Vector = null
  var queries:RDD[Query] = null

  /* Functions */
  // Generate the data file that ZhangZhen wants
  def generateDataFile(sc:SparkContext, filePath:String, savePath:String): Unit ={
    val textFile = sc.textFile(filePath)

    // Get the Max Value of Each Feature
    val features = textFile.map(line => line.drop(line.indexOf("1:")).split(" "))

    // val t = features.collect()

    val featuresPair = features.flatMap(fs => for (f <- fs) yield {
        new Tuple2[Int, Double](f.split(":")(0).toInt, f.split(":")(1).toDouble)
    })
    val maxFeatures = featuresPair.reduceByKey((x, y) => if(math.abs(x) > math.abs(y)) math.abs(x) else math.abs(y))
    var maxFeaturesArray = maxFeatures.collect()
    Sorting.quickSort(maxFeaturesArray)(Ordering.by[(Int, Double), Int](_._1))
    val maxValueOfFeature:Array[Double] = for(maxFeature <- maxFeaturesArray) yield {maxFeature._2}

    // Map Transformation - Key-Value: (query ID, line)
    val mappedData = textFile.map(
      line => new Tuple2[String, String](line.split(" ")(1).split(":")(1),
        line))

    // ReduceByKey Transformation
    val reducedData = mappedData.groupByKey()
      .map(x => new Query(x._1.toInt, x._2.size, (for(v:String <- x._2) yield{
      v.split(" ")(0).toInt
    }).toArray,
      (for(line:String <- x._2) yield{
        // Extra features
        val elements:ArrayBuffer[Double] = new ArrayBuffer[Double]()
        var newLine:String = line.drop(line.indexOf("1:"))
        if(line.indexOf("#") != -1) {
          newLine = newLine.dropRight(newLine.length - newLine.indexOf("#") + 1)
        }
        var i = 0
        for(feature <- newLine.split(" ")){
          var t = 1.0
          if(maxValueOfFeature(i) > 0)
            t = maxValueOfFeature(i)

          val value = feature.split(":")(1).toDouble / t
          elements .+= (value)
          i = i + 1
        }
        val vector:Vector = new Vector(elements.toArray)
        vector
      }).toArray))

    queries = reducedData

    val newTrainRDD = queries.flatMap(query => for(i <- 0 to query.numberOfDocuments - 1) yield {
      var line:String = if(query.realScores(i) > 0) "1 " else "0 "
      for(j <- 0 to query.docFeatures(i).length - 1){
        val s = new java.text.DecimalFormat("#.##").format(query.docFeatures(i)(j) )
        line = line + s
        if(j != query.docFeatures(i).length - 1)
          line = line + " "
      }
      line
    })

    newTrainRDD.saveAsTextFile(savePath)
  }

  // Generate the test file that ZhangZhen wants
  def generateTestFile(sc:SparkContext, filePath:String, savePath:String): Unit ={
    val textFile = sc.textFile(filePath)

    // Get the Max Value of Each Feature
    val features = textFile.map(line => line.drop(line.indexOf("1:")).split(" "))
    val featuresPair = features.flatMap(fs => for (f <- fs) yield {
      new Tuple2[Int, Double](f.split(":")(0).toInt, f.split(":")(1).toDouble)
    })
    val maxFeatures = featuresPair.reduceByKey((x, y) => if(math.abs(x) > math.abs(y)) math.abs(x) else math.abs(y))
    var maxFeaturesArray = maxFeatures.collect()
    Sorting.quickSort(maxFeaturesArray)(Ordering.by[(Int, Double), Int](_._1))
    val maxValueOfFeature:Array[Double] = for(maxFeature <- maxFeaturesArray) yield {maxFeature._2}

    // Map Transformation - Key-Value: (query ID, line)
    val mappedData = textFile.map(
      line => new Tuple2[String, String](line.split(" ")(1).split(":")(1),
        line))

    // ReduceByKey Transformation
    val reducedData = mappedData.groupByKey()
      .map(x => new Query(x._1.toInt, x._2.size, (for(v:String <- x._2) yield{
      v.split(" ")(0).toInt
    }).toArray,
      (for(line:String <- x._2) yield{
        // Extra features
        val elements:ArrayBuffer[Double] = new ArrayBuffer[Double]()
        var newLine:String = line.drop(line.indexOf("1:"))
        if(line.indexOf("#") != -1) {
          newLine = newLine.dropRight(newLine.length - newLine.indexOf("#") + 1)
        }
        var i = 0
        for(feature <- newLine.split(" ")){
          var t = 1.0
          if(maxValueOfFeature(i) > 0)
            t = maxValueOfFeature(i)

          val value = feature.split(":")(1).toDouble / t
          elements .+= (value)
          i = i + 1
        }
        // val elementsWithWZero:ArrayBuffer[Double] = ArrayBuffer(1.0) ++: elements
        val vector:Vector = new Vector(elements.toArray)
        vector
      }).toArray))

    queries = reducedData

    var newTrainRDD = queries.flatMap(query => for(i <- 0 to query.numberOfDocuments - 1) yield{
      var line = query.queryID.toString + "#" + i.toString + ":"
      for(j <- 0 to query.docFeatures(i).length - 1){
        val s = new java.text.DecimalFormat("#.##").format(query.docFeatures(i)(j))
        line = line + s
        if(j != query.docFeatures(i).length - 1)
          line = line + ","
      }
      line
    })

    newTrainRDD.saveAsTextFile(savePath)
  }

  // Read & Initialize Data Set
  def initData(sc:SparkContext, filePath:String):RDD[Query] = {
    val textFile = sc.textFile(filePath)

    // Get the Max Value of Each Feature
    val features = textFile.map(line => line.drop(line.indexOf("1:")).split(" "))
    val featuresPair = features.flatMap(fs => for (f <- fs) yield {
      new Tuple2[Int, Double](f.split(":")(0).toInt, f.split(":")(1).toDouble)
    })
    val maxFeatures = featuresPair.reduceByKey((x, y) => if(math.abs(x) > math.abs(y)) math.abs(x) else math.abs(y))
    var maxFeaturesArray = maxFeatures.collect()
    Sorting.quickSort(maxFeaturesArray)(Ordering.by[(Int, Double), Int](_._1))
    val maxValueOfFeature:Array[Double] = for(maxFeature <- maxFeaturesArray) yield {maxFeature._2}

    // Map Transformation - Key-Value: (query ID, line)
    val mappedData = textFile.map(
        line => new Tuple2[String, String](line.split(" ")(1).split(":")(1),
        line))

    // ReduceByKey Transformation
    val reducedData = mappedData.groupByKey()
        .map(x => new Query(x._1.toInt, x._2.size, (for(v:String <- x._2) yield{
          v.split(" ")(0).toInt
        }).toArray,
      (for(line:String <- x._2) yield{
          // Extra features
          val elements:ArrayBuffer[Double] = new ArrayBuffer[Double]()
          val newLine:String = line.drop(line.indexOf("1:")).dropRight(1)
          // val newDropedLine:String = newLine.dropRight(newLine.length - newLine.indexOf("#") + 1)
          // val newDropedLine:String = newLine
          var i = 0
          for(feature <- newLine.split(" ")){
            val value = if(maxValueOfFeature(i) == 0) feature.split(":")(1).toDouble else feature.split(":")(1).toDouble / maxValueOfFeature(i)
            elements .+= (value)
            i = i + 1
          }
          val elementsWithWZero:ArrayBuffer[Double] = ArrayBuffer(1.0) ++: elements
          val vector:Vector = new Vector(elementsWithWZero.toArray)
          vector
        }).toArray))

    queries = reducedData
    queries.cache()
    queries
  }

  // Print Queries
  def printQueries(queries:RDD[Query]): Unit ={
    println(queries.count)
    for(q <- queries) {
      println(q.queryID)
      println(q.numberOfDocuments)
      println(q.realScores)
      println(q.docFeatures)
    }
  }

  // Model Training
  def trainModel(sc:SparkContext, queries:RDD[Query]):Vector = {
    // Gradient Descent Algorithm
    var w:Vector = Vector.zeros(DIM)

    for(i <- 1 to ITERATIONS){
      val gradient = sc.accumulator(Vector.zeros(DIM))
      val loss = sc.accumulator(0.0)
      for (q <- queries) {
        val expRelScores = q.realScores.map(y => math.exp(y.toDouble))
        val ourScores = q.docFeatures.map(x => w dot x)
        val expOurScores = ourScores.map(z => math.exp(z))
        val sumExpRelScores = expRelScores.reduce(_ + _)
        val sumExpOurScores = expOurScores.reduce(_ + _)
        val P_y = expRelScores.map(y => y/sumExpRelScores)
        val P_z = expOurScores.map(z => z/sumExpOurScores)
        var lossForAQuery = 0.0
        var gradientForAQuery = Vector.zeros(DIM)
        for (j <- 0 to q.realScores.size - 1) {
          gradientForAQuery += (q.docFeatures(j) * (P_z(j) - P_y(j)))
          lossForAQuery += -P_y(j) * math.log(P_z(j))
        }
        gradient += gradientForAQuery
        loss += lossForAQuery   // Loss Function
      }
      w -= gradient.value * STEP_SIZE
    }
    this.model = w
    w
  }

  // Save Model To an External File
  def saveModel(sc:SparkContext, path:String): Unit ={
    /*
    val out = new PrintWriter(path)
    // println(this.model.length)
    for(i <- 0 to this.model.length - 1){
      out.print(this.model(i).toString + " ")
    }
    out.close()
    */

    val modelArray = for(i <- 0 to this.model.length - 1) yield {this.model(i)}
    val modelRDD = sc.parallelize(modelArray)
    modelRDD.saveAsTextFile(path)
  }


  // Generate List
  def generateList(query: Query):Array[(Int)] = {
    var i = -1
    val scores:Array[(Int, Double)] = for(feature <- query.docFeatures) yield {i = i + 1; (i, this.model dot feature)}
    Sorting.quickSort(scores)(Ordering.by[(Int, Double), Double](_._2))
    val urls = for(score <- scores) yield {score._1}
    urls
  }

  def parsePointWiseOutput(sc:SparkContext, filePath:String, savePath:String): Unit ={
    val textFile = sc.textFile(filePath)

    val result01 = textFile.map(line => (line.split("\t\\{0-1=")(0).toInt,
        line.drop(line.indexOf("[") + 1).dropRight(line.length - line.indexOf("], LIR")).split(", ")))
    val resultLIR = textFile.map(line => (line.split("\t\\{0-1=")(0).toInt,
      line.drop(line.indexOf("[") + 1).dropRight(line.length - line.indexOf("], LIR")).split(", ")))
    val resultLR = textFile.map(line => (line.split("\t\\{0-1=")(0).toInt,
      line.drop(line.indexOf("[") + 1).dropRight(line.length - line.indexOf("], LIR")).split(", ")
    ))
    val result10 = textFile.map(line => (line.split("\t\\{0-1=")(0).toInt,
      line.drop(line.indexOf("[") + 1).dropRight(line.length - line.indexOf("], LIR")).split(", ")))

    val sortedQueries = queries.sortBy(query => query.queryID, true)
    val sortedResult01 = result01.sortByKey(true)
    val sortedResultLIR = resultLIR.sortByKey(true)
    val sortedResultLR = resultLR.sortByKey(true)
    val sortedResult10 =result10.sortByKey(true)

    val t1 = sortedResult01.collect()
    val t2 = sortedResultLIR.collect()
    val t3 = sortedResultLR.collect()
    val t4 = sortedResult10.collect()
    val i = 3

    /*
    val ndcgOfsortedResult01 = sortedQueries.map(query =>
      NDCG.getNDCG(toIntegerList(
        sortedResult01.filter((x, y) => x == query.queryID).collect()(0)._2.toList),
        toIntegerList(oriUrls.toList), 5))
        */
  }

  def saveAvgNDCG(sc:SparkContext, ndcgFilePath:String):Unit = {
    val n = queries.map(realQuery => calcNDCG(generateList(realQuery), realQuery)).cache()
    val count = n.count()
    val sum = n.reduce(_ + _)
    val r = sc.parallelize(Array(sum / count))
    r.saveAsTextFile(ndcgFilePath)
  }

  // Calculate All the NDCG Values
  def calcNDCGs(): Array[Double] = if(this.model == null)
    null
  else {
    queries.map(realQuery => calcNDCG(generateList(realQuery), realQuery)).collect()
    /*
    val quriesArray = this.queries.collect()
    val ndcgs: Array[Double] = for (realQuery <- quriesArray) yield {
      val myUrls = generateList(realQuery)
      calcNDCG(myUrls, realQuery)
    }
    */
    // ndcgs
  }

  // Save NDCG Values
  def saveNDCGValues(sc:SparkContext, ndcgFilePath:String, ndcgs:Array[Double]): Unit ={
    /*
    val out = new PrintWriter(ndcgFilePath)
    for(i <- 0 to ndcgs.length - 1){
      out.print(ndcgs(i).toString + " ")
    }
    out.close()
    */

    val modelRDD = sc.parallelize(ndcgs)
    modelRDD.saveAsTextFile(ndcgFilePath)
  }

  // Calculate NDCG Value
  def calcNDCG(myUrl:Array[Int], realQuery:Query):Double = {
    var i = -1
    val realScores:Array[(Int, Double)] = for(score <- realQuery.realScores) yield {i = i + 1; (i, score.toDouble)}
    Sorting.quickSort(realScores)(Ordering.by[(Int, Double), Double](_._2))
    val oriUrls = for(score <- realScores) yield {score._1}     // real orders
    val ndcg = NDCG.getNDCG(toIntegerList(myUrl.toList), toIntegerList(oriUrls.toList), 5)
    ndcg
  }

  // Convert Scala List to Java List
  implicit def toIntegerList(lst: List[Int] ) =
    seqAsJavaList( lst.map( i => i:java.lang.Integer))


  // Read Model From External file
  def readModel(sc:SparkContext, path:String): Unit ={
    /*
    val source = scala.io.Source.fromFile(path, "UTF-8")
    val lines = source.getLines().toArray
    val modelArray = for(w <- lines(0).split(" ")) yield {w.toDouble}
    */
    val textFile = sc.textFile(path)
    val modelArray = for(line <- textFile.collect()) yield{
      line.toDouble
    }
    this.model = new Vector(modelArray)
  }
}