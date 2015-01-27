/**
 * Created by ihainan on 1/22/15.
 */
import java.io.File

object Loo {
  def main(args: Array[String]) {
    def recursiveListFiles(f: File): Array[String] = {
      val these = f.listFiles
      these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
      val fileNames:Array[String] = these.map(f => f.getName)
      fileNames
    }

    val fileNames:Array[String] = recursiveListFiles(new File("/Users/ihainan/tmp"))
    for(fileName <- fileNames)
      print(fileName + " ")
  }
}
