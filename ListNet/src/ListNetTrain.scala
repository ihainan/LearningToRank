import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ihainan on 1/24/15.
 */
object ListNetTrain extends Serializable{
  def main(args: Array[String]) {
    // Get Application Parameters
    val trainingDataPath = args(0)
    val modelPath = args(1)
    val iterators = args(2).toInt
    val alpha = args(3).toDouble


    // Init Spark && ListNet Model
    val conf = new SparkConf().setAppName("ListNet Model Train " + args(2) + " " + args(3))
    val sc = new SparkContext(conf);
    println("Info: Initial Apache Spark success")

    val listNet = new ListNet(iterators, alpha)
    println("Info: Initial ListNet Algorithm Model success")

    // Read & Format Input File
    val queries = listNet.initData(sc, trainingDataPath)

    // Train Model
    listNet.trainModel(sc, queries)

    // Save Model
    listNet.saveModel(sc, modelPath)
  }
}
