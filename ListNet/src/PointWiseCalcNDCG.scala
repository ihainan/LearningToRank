import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ihainan on 1/26/15.
 */
object PointWiseCalcNDCG {
  def main(args: Array[String]) {
    // Get Application Parameters
    val trainingDataPath = args(0)
    val testOutputPath = args(1)
    val saveOutputPath = args(2)

    val iterators = args(3).toInt
    val alpha = args(4).toDouble

    val conf = new SparkConf().setAppName("PointWiseCalcNDCG" + args(2) + " " + args(3))
    val sc = new SparkContext(conf)
    val listNet = new ListNet(iterators, alpha)

    listNet.initData(sc, trainingDataPath)
    listNet.parsePointWiseOutput(sc, testOutputPath, saveOutputPath)

  }
}
