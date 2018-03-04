
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame

// Создание и обучение модели
object MLModel extends Serializable {
  def createModelAndPredict(dataFrame: DataFrame): Unit = {
    // делим входыне данные на train и test
    val splitData = dataFrame.randomSplit(Array(0.7, 0.3))
    val trainData = splitData(0)
    val testData = splitData(1)

    // обучаем логистическую регрессию: X - признаки после CountVectorizer, Y - индекс языка
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val lrModel = lr.fit(trainData.select("label", "features"))

    println(s"Coefficients: \n${lrModel.coefficientMatrix}")
    println(s"Intercepts: \n${lrModel.interceptVector}")

    val prediction = lrModel.transform(testData)
    val predictionAndLabels = prediction.select("prediction", "label")

    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
  }
}
