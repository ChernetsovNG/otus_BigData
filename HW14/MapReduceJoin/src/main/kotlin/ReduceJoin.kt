import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashSet


private const val FIRST_MONTH_FILENAME = "first_month_top_pairs_tab.txt"
private const val SECOND_MONTH_FILENAME = "clickstream-enwiki-2017-12.tsv.gz"

// Класс отвечает за подсчёт за подсчёт пар за второй месяц с учётом результатов топ пар в первый месяц
class ReduceJoin : Configured(), Tool {
    companion object {
        val logger = LoggerFactory.getLogger(ReduceJoin::class.java)
    }

    // Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
    class Map : Mapper<LongWritable, Text, Text, Text>() {
        // читаем в маппере топ-10000 пар за первый месяц и данные за второй месяц
        override fun map(key: LongWritable, value: Text, context: Context) {
            val fileName = (context.inputSplit as FileSplit).path.name

            val inputString = value.toString()

            // разделитель в строках - табуляция
            val tokenizer = StringTokenizer(inputString, "\t")

            if (tokenizer.countTokens() == 2) {  // прочитали данные из топ-10000 за первый месяц
                val wordPair = tokenizer.nextToken()
                val count = Integer.parseInt(tokenizer.nextToken())

                value.set(wordPair)
                context.write(value, Text("$fileName $count"))  // выход mapper'а. Сохраняем ссылку на источник - первый (first) месяц
            } else if (tokenizer.countTokens() == 4) {  // прочитали данные за второй месяц
                val firstWord = tokenizer.nextToken()
                val secondWord = tokenizer.nextToken()
                val type = tokenizer.nextToken()
                val count = Integer.parseInt(tokenizer.nextToken())

                val wordPair = "$firstWord $secondWord"

                value.set(wordPair)
                context.write(value, Text("$fileName $count"))  // сохраняем ссылку на источник - второй месяц
            }
        }
    }

    // Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
    class Reduce : Reducer<Text, Text, Text, IntWritable>() {
        override fun reduce(key: Text, values: MutableIterable<Text>, context: Context) {
            val valuesList = values.map { it -> it.toString() }.toList()
            if (valuesList.size > 1) {
                logger.info("values_len=${valuesList.size}, key=$key, values=$valuesList")

                val sources = HashSet<String>()  // сохраним источники в множестве, чтобы понять, откуда пришли данные
                val secondMonthCounts = ArrayList<Int>()

                for (value in valuesList) {
                    val splitText = value.split(" ")
                    val source = splitText[0]
                    val count = Integer.parseInt(splitText[1])
                    sources.add(source)
                    if (source.contains("clickstream-enwiki")) {
                        secondMonthCounts.add(count)
                    }
                }

                logger.info("sources=$sources, secondMonthCounts=$secondMonthCounts")
                // Проверяем, что данные пришли с обоих источников (из первого и второго месяцев)
                if (sources.size > 1) {
                    // суммируем все количества за второй месяц
                    val sum = secondMonthCounts.sum()
                    context.write(key, IntWritable(sum))
                }
            }
            // иначе пропускаем эту пару слов
        }
    }

    @Override
    override fun run(args: Array<String>): Int {
        val job = Job.getInstance(conf, ReduceJoin::class.java.canonicalName)

        job.setJarByClass(ReduceJoin::class.java)
        job.mapperClass = ReduceJoin.Map::class.java
        job.reducerClass = ReduceJoin.Reduce::class.java

        job.mapOutputKeyClass = Text::class.java
        job.mapOutputValueClass = Text::class.java

        job.outputKeyClass = Text::class.java
        job.outputValueClass = Text::class.java
        job.inputFormatClass = TextInputFormat::class.java
        job.outputFormatClass = TextOutputFormat::class.java

        val outputPath = Path(args[1])

        FileInputFormat.addInputPath(job, Path(args[0]))
        FileOutputFormat.setOutputPath(job, outputPath)

        outputPath.getFileSystem(conf).delete(outputPath, true)

        return if (job.waitForCompletion(true)) 0 else 1
    }
}
