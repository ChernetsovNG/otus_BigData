
import org.slf4j.{Logger, LoggerFactory}

// Вспомогательный объект, чтобы писать логи при запуске в Spark
object LogHolder extends Serializable {
  @transient lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
}
