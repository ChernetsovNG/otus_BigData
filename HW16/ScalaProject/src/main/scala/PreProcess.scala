
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, split, udf}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable


// Подготовка данных для обучения модели
object PreProcess extends Serializable {

  // Пользовательские функции для преобразований Spark RDD
  val filterSetByLanguages: UserDefinedFunction = udf { (a: mutable.WrappedArray[String]) => a.intersect(StackOverflowDataset.languages) }
  val setCount: UserDefinedFunction = udf { (a: mutable.WrappedArray[String]) => a.length }
  val getLanguageIndex: UserDefinedFunction = udf { (a: String) => StackOverflowDataset.indexByColName(a) }

  def prepareDataset(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    // Читаем файл с HDFS
    val stackOverflowDF = readCSVFile(sparkSession, StackOverflowDataset.inputFile)

    // Делим теги по пробелу и оставляем только языки
    val stackOverflowDFTags = stackOverflowDF
      .withColumn("TagsArray", split(col("Tags"), " "))

    val stackOverflowDFTagsFiltered = stackOverflowDFTags
      .withColumn("TagsFiltered", filterSetByLanguages(col("TagsArray")))

    // Оставляем только те строки, где в тегах есть один язык
    val stackOverflowDFTagsLength = stackOverflowDFTagsFiltered
      .withColumn("TagsLength", setCount(col("TagsFiltered")))

    val stackOverflowDFTagsLengthFiltered = stackOverflowDFTagsLength
      .filter(col("TagsLength") === 1).withColumn("Tag", $"TagsFiltered".getItem(0))

    // Оставляем вместо имени языка его индекс - целевой признак
    val stackOverflowDSIndexed = stackOverflowDFTagsLengthFiltered
      .withColumn("label", getLanguageIndex(col("Tag")))
      .drop("TagsArray").drop("Tags").drop("TagsFiltered").drop("TagsLength").drop("Tag")

    // Разбиваем столбцы Title и Body на отдельные слова, удаляем стоп-слова
    val regexTokenizer = new RegexTokenizer().setInputCol("Title").setInputCol("Body").setOutputCol("words").setPattern("\\W")
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("wordsFiltered")

    val regexTokenized = regexTokenizer.transform(stackOverflowDSIndexed)
    val regexTokenizedFiltered = remover.transform(regexTokenized)

    // Применяем CountVectorizer (составляем "мешок слов" и вычисляем количество вхождений каждого слова
    // для каждого образца во входных данных
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("wordsFiltered").setOutputCol("features").setVocabSize(1000).setMinDF(10)
      .fit(regexTokenizedFiltered)

    val dataset = cvModel.transform(regexTokenizedFiltered)
      .drop("Title").drop("Body").drop("words").drop("wordsFiltered")
      .withColumn("labelInt", $"label".cast(IntegerType)).drop("label").withColumnRenamed("labelInt", "label")

    // Удаляем файл, если он уже был
    val fs = FileSystem.get(new URI(StackOverflowDataset.outputFolder), sparkSession.sparkContext.hadoopConfiguration)
    fs.delete(new Path(StackOverflowDataset.outputDatasetPath), true)
    dataset.write.format("json").save(StackOverflowDataset.outputDatasetPath)

    dataset
  }

  def readCSVFile(sparkSession: SparkSession, fileName: String): DataFrame = {
    sparkSession.read.format("csv")
      .option("header", "true").option("escape", "\"").option("multiLine", "true")
      .load(fileName)
  }

}
