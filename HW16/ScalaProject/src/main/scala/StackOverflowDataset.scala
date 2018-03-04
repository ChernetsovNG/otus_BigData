
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split, udf}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable.WrappedArray


object StackOverflowDataset {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().appName("StackOverflowCreateDataset").getOrCreate()
    import sparkSession.implicits._

    val inputFile = "/HW16/data/Train.csv"
    val outputTitles = "/HW16/output/stackoverflow_titles_languages"
    val outputDatasetPath = "/HW16/output/stackoverflow_dataset_count"

    val stackOverflowDF = sparkSession.read.format("csv")
      .option("header", "true").option("escape", "\"").option("multiLine", "true")
      .load(inputFile)

    val languages = Array("javascript", "java", "python", "ruby", "php",
      "c++", "c#", "go", "scala", "swift")

    val filterSet = udf { (a: WrappedArray[String]) => a.intersect(languages) }
    val setCount = udf { (a: WrappedArray[String]) => a.length }

    val stackOverflowDFTags = stackOverflowDF
      .withColumn("TagsArray", split(col("Tags"), " "))

    val stackOverflowDFTagsFiltered = stackOverflowDFTags
      .withColumn("TagsFiltered", filterSet(col("TagsArray")))

    val stackOverflowDFTagsLength = stackOverflowDFTagsFiltered
      .withColumn("TagsLength", setCount(col("TagsFiltered")))
    val stackOverflowDFTagsLengthFiltered = stackOverflowDFTagsLength
      .filter(col("TagsLength") === 1).withColumn("Tag", $"TagsFiltered".getItem(0))

    val indexByColName = languages.zipWithIndex.toMap
    val get_index = udf { (a: String) => indexByColName(a) }

    val stackOverflowDS = stackOverflowDFTagsLengthFiltered
      .withColumn("label", get_index(col("Tag")))
      .drop("TagsArray").drop("Tags").drop("Body").drop("TagsFiltered").drop("TagsLength").drop("Tag")

    val regexTokenizer = new RegexTokenizer().setInputCol("Title").setOutputCol("words").setPattern("\\W")
    val regexTokenized = regexTokenizer.transform(stackOverflowDS)

    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("wordsFiltered")
    val regexTokenizedFiltered = remover.transform(regexTokenized)

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("wordsFiltered").setOutputCol("features").setVocabSize(1000).setMinDF(10)
      .fit(regexTokenizedFiltered)

    val dataset = cvModel.transform(regexTokenizedFiltered)
      .drop("Title").drop("words").drop("wordsFiltered")
      .withColumn("labelInt", $"label".cast(IntegerType)).drop("label").withColumnRenamed("labelInt", "label")

    dataset.write.format("json").save(outputDatasetPath)

    val splitData = dataset.randomSplit(Array(0.7, 0.3))
    val trainData = splitData(0)
    val testData = splitData(1)

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val lrModel = lr.fit(trainData.select("label", "features"))

    println(s"Coefficients: \n${lrModel.coefficientMatrix}")
    println(s"Intercepts: \n${lrModel.interceptVector}")

    val prediction = lrModel.transform(testData.select("features"))
    val predictionAndLabels = prediction.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")

    sparkSession.stop()
  }
}
