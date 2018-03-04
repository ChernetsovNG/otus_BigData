
import org.apache.spark.sql.SparkSession

object StackOverflowDataset extends Serializable {
  // Пути к файлам на HDFS
  val inputFile = "hdfs://localhost:9000/HW16/data/Train.csv"
  val outputTitles = "hdfs://localhost:9000/HW16/output/stackoverflow_titles_languages"
  val outputDatasetPath = "hdfs://localhost:9000/HW16/output/stackoverflow_dataset_count"

  // Языки программирования в тегах - целевые переменные
  val languages = Array("javascript", "java", "python", "ruby", "php", "c++", "c#", "go", "scala", "swift")
  val indexByColName: Map[String, Int] = languages.zipWithIndex.toMap


  // Пример запуска в Spark
  // sbt package
  // spark-submit --class "StackOverflowDataset" /home/n_chernetsov/Dropbox/Education/Otus_BigData/otus_BigData/HW16/ScalaProject/target/scala-2.11/scalaproject_2.11-0.1.jar
  def main(args: Array[String]) {
    val sparkSession = createSparkSession("StackOverflowCreateDataset")
    val dataset = PreProcess.prepareDataset(sparkSession)
    MLModel.createModelAndPredict(dataset)
    sparkSession.stop()
  }

  // Создать сессию Spark
  def createSparkSession(appName: String): SparkSession =
    SparkSession.builder().appName(appName).getOrCreate()

}


