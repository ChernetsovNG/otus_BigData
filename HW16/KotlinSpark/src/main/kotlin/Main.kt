import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import scala.collection.Set
import scala.collection.mutable.WrappedArray

object StackOverflowDataset {

    @JvmStatic
    fun main(args: Array<String>) {
        val sparkSession = SparkSession.builder().appName("StackOverflowCreateDataset").orCreate

        val inputFile = "/HW16/data/Train.csv"
        val outputFile = args[1]

        val stackOverflowDF = sparkSession.read().format("csv")
            .option("header", "true")
            .option("escape", "\"")
            .option("multiLine", "true").load(inputFile)

        val languages = Set("javascript", "java", "python", "ruby", "php", "c++", "c#", "go", "scala", "swift")

        // из столбца с тегами делаем целевую переменную
        val stackOverflowDFTags = stackOverflowDF.withColumn("TagsArray", split(col("Tags"), " "))
        val filterSet = fun(a: WrappedArray<String>) =
            a.toSet<String>().intersect(languages).toList()

        val stackOverflowDFTagsFiltered = stackOverflowDFTags.withColumn("TagsFiltered", filterSet(col("TagsArray")))
        val setCount = udf { (a: WrappedArray[String]) => a.length }
    }
}