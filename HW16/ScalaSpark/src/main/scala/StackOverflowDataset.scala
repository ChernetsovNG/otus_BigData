




// spark-submit --class "StackOverflowDataset" target/scala-2.11/simple-project_2.11-1.0.jar gs://otusspark/data/stackoverflow/Train.csv gs://otusspark/data_output/stackoverflow_dataset

object StackOverflowDataset {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("StackOverflowCreateDataset").getOrCreate()

    val inputFile = args(0)
    val outputPath = args(1)

    val inputFile = "gs://otusspark/data/stackoverflow/Train.csv"
    val outputTitles = "gs://otusspark/data_output/stackoverflow_titles_languages"
    val outputDatasetPath = "gs://otusspark/data_output/stackoverflow_dataset_count"

    val stackOverflowDF = spark.read.format("csv").option("header", "true").option("escape", "\"").option("multiLine", "true").load(inputFile)
    val languages = Set("javascript", "java", "python", "ruby", "php", "c++", "c#", "go", "scala", "swift")

    val stackOverflowDFTags = stackOverflowDF.withColumn("TagsArray", split(col("Tags"), " "))
    val filterSet = udf { (a: WrappedArray[String]) => a.toSet.intersect(languages).toSeq }
    val stackOverflowDFTagsFiltered = stackOverflowDFTags.withColumn("TagsFiltered", filterSet(col("TagsArray")))
    val setCount = udf { (a: WrappedArray[String]) => a.length }

    val stackOverflowDFTagsLength = stackOverflowDFTagsFiltered.withColumn("TagsLength", setCount(col("TagsFiltered")))
    val stackOverflowDFTagsLengthFiltered = stackOverflowDFTagsLength.filter(col("TagsLength") === 1).withColumn("Tag", $"TagsFiltered".getItem(0))

    val indexByColName = languages.zipWithIndex.toMap
    val get_index = udf { (a: String) => indexByColName(a) }
    val stackOverflowDS = stackOverflowDFTagsLengthFiltered.withColumn("label", get_index(col("Tag"))).drop("TagsArray").drop("Tags").drop("Body").drop("TagsFiltered").drop("TagsLength").drop("Tag")
    stackOverflowDS.write.format("csv").option("header", "true").option("escape", "\"").option("multiLine", "true").save(outputTitles)

    val stackOverflowDS = spark.read.format("csv").option("header", "true").option("escape", "\"").option("multiLine", "true").load(outputTitles)

    val regexTokenizer = new RegexTokenizer().setInputCol("Title").setOutputCol("words").setPattern("\\W")
    val regexTokenized = regexTokenizer.transform(stackOverflowDS)

    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("wordsFiltered")
    val regexTokenizedFiltered = remover.transform(regexTokenized)

    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("wordsFiltered").setOutputCol("features").setVocabSize(1000).setMinDF(10).fit(regexTokenizedFiltered)
    val dataset = cvModel.transform(regexTokenizedFiltered).drop("Title").drop("words").drop("wordsFiltered").withColumn("labelInt", $"label".cast(IntegerType)).drop("label").withColumnRenamed("labelInt", "label")

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

    spark.stop()
  }
}
