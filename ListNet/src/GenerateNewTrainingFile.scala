/**
 * Created by ihainan on 1/24/15.
 */
import org.apache.spark.{SparkContext, SparkConf}

object GenerateNewTrainingFile {
  def main(args: Array[String]) {
    // Get Application Parameters
    val trainingDataPath = args(0)
    val newTrainingDataPath = args(1)
    val iterators = args(2).toInt
    val alpha = args(3).toDouble

    val conf = new SparkConf().setAppName("Generate New Training File" + args(2) + " " + args(3))
    val sc = new SparkContext(conf)
    val listNet = new ListNet(iterators, alpha)

    // listNet.generateTestFile(sc, trainingDataPath, newTrainingDataPath)
    // listNet.initData(sc, trainingDataPath)
  }
}
