import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ihainan on 1/24/15.
 */
object ListNetEvalution {
  def main(args: Array[String]) {
    // Get Application Parameters
    val testDataPath = args(0)
    val modelPath = args(1)
    val ndcgFilePath = args(2)
    val iterators = args(3).toInt
    val alpha = args(4).toDouble

    // Init Spark && ListNet Model
    val conf = new SparkConf().setAppName("ListNet Model Evaluation " + args(3) + " " + args(4))
    val sc = new SparkContext(conf);
    println("Info: Initial Apache Spark success")
    val listNet = new ListNet(iterators, alpha)

    // Read Model from file
    listNet.readModel(sc, modelPath)

    // Read Test Data
    listNet.initData(sc, testDataPath)

    // Get NDCG Value & Save into file
    // val ndcgs = listNet.calcNDCGs()
    // listNet.saveNDCGValues(sc, ndcgFilePath, ndcgs)
    listNet.saveAvgNDCG(sc, ndcgFilePath)
  }
}