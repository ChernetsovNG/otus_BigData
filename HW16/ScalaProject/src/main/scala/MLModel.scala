
import ModelType.ModelType
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, Dataset, Row}


object ModelType extends Enumeration {
  type ModelType = Value
  val LOGISTIC_REGRESSION, RANDOM_FOREST = Value
}


// Создание и обучение модели
object MLModel extends Serializable {

  def createModelAndPredict(dataFrame: DataFrame, modelType: ModelType) {
    // делим входыне данные на train и test
    val splitData = dataFrame.randomSplit(Array(0.7, 0.3))
    val trainData = splitData(0)
    val testData = splitData(1)

    modelType match {
      case ModelType.LOGISTIC_REGRESSION => logisticRegression(trainData, testData)
      case ModelType.RANDOM_FOREST => randomForest(trainData, testData)
    }
  }

  def logisticRegression(trainData: Dataset[Row], testData: Dataset[Row]) {
    val logReg = new LogisticRegression()

    // Поиск оптимальных параметров модели
    val paramGrid = new ParamGridBuilder()
      .addGrid(logReg.maxIter, Array(10, 100, 200))
      .addGrid(logReg.regParam, Array(0.0, 0.3, 1.0))
      .addGrid(logReg.elasticNetParam, Array(0.0, 0.8))
      .build()

    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")

    val cv = new CrossValidator()
      .setEstimator(logReg)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
      .setParallelism(4)

    // Кросс-валидация и поиск лучших параметров.
    val cvModel = cv.fit(trainData.select("label", "features"))

    println(s"Best model params: ${cvModel.bestModel.params}")

    val prediction = cvModel.transform(testData)
    val predictionAndLabels = prediction.select("prediction", "label")

    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
    // Test set accuracy = 0.6956575989909941
  }

  def randomForest(trainData: Dataset[Row], testData: Dataset[Row]) {
    val randomForestClassifier = new RandomForestClassifier().setNumTrees(100)

    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")

    /*// Поиск оптимальных параметров модели
    val paramGrid = new ParamGridBuilder()
      .addGrid(randomForestClassifier.numTrees, Array(100))
      .build()

    val cv = new CrossValidator()
      .setEstimator(randomForestClassifier)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
      .setParallelism(4)

    // Кросс-валидация и поиск лучших параметров.
    val cvModel = cv.fit(trainData.select("label", "features"))

    println(s"Best model params: ${cvModel.bestModel.params}")*/

    val randomForestModel = randomForestClassifier.fit(trainData.select("label", "features"))

    val prediction = randomForestModel.transform(testData)
    val predictionAndLabels = prediction.select("prediction", "label")

    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
    // Test set accuracy = 0.6627383588465346
  }

}
