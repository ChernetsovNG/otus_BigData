import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import java.util.*

class WordPairCount {
    // Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
    // файл предобработан так, что в каждой строке - отдельное предложение
    class Map : Mapper<LongWritable, Text, Text, IntWritable>() {
        val stopwords = listOf("the", "and", "to", "of", "a", "at", "on", "in", "was",
            "as", "i", "had", "be", "into", "it", "so")

        override fun map(key: LongWritable, value: Text, context: Context) {
            val inputSentence = value.toString()

            val wordPairs = getWordPairs(inputSentence)

            for (pair in wordPairs) {
                value.set(pair)
                context.write(value, IntWritable(1))  // выход mapper'а
            }
        }

        // получить список пар слов в предложении
        private fun getWordPairs(sentence: String): List<String> {
            val tokenizer = StringTokenizer(sentence)
            val sentenceTokens = ArrayList<String>()
            while (tokenizer.hasMoreTokens()) {
                sentenceTokens.add(tokenizer.nextToken())
            }
            // Очищаем слова от несловарных символов и повторяющихся пробелов, удаляем стоп-слова
            val clearSentenceTokens = sentenceTokens.map { token -> clearToken(token) } as ArrayList
            clearSentenceTokens.removeAll(stopwords)
            clearSentenceTokens.remove("")


            // Составляем все возможные пары слов в предложении, разделённые пробелом
            return getListOfPairs(clearSentenceTokens)
        }

        private fun clearToken(token: String): String {
            return token.toLowerCase()
                .replace(Regex("[^a-zA-Zа-яА-Я0-9]"), "")
                .replace(Regex("[\\s]{2,}"), " ")
        }

        private fun getListOfPairs(list: List<String>): List<String> {
            val listOfPairs = ArrayList<String>()
            for ((index1, word1) in list.withIndex()) {
                for ((index2, word2) in list.withIndex()) {
                    if (index1 != index2) {
                        listOfPairs.add("$word1 $word2")
                    }
                }
            }
            return listOfPairs
        }
    }

    // Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
    class Reduce : Reducer<Text, IntWritable, Text, IntWritable>() {
        override fun reduce(key: Text, values: MutableIterable<IntWritable>, context: Context) {
            val sum = values.sumBy { it.get() }
            context.write(key, IntWritable(sum))
        }
    }
}

// пример вызова задачи:
// hadoop jar WordPairCount.jar /input /output
fun main(args: Array<String>) {
    val conf = Configuration()
    val job = Job.getInstance(conf, "WordPairCount")

    job.setJarByClass(WordPairCount::class.java)
    job.mapperClass = WordPairCount.Map::class.java
    job.reducerClass = WordPairCount.Reduce::class.java

    job.outputKeyClass = Text::class.java
    job.outputValueClass = IntWritable::class.java

    job.inputFormatClass = TextInputFormat::class.java
    job.outputFormatClass = TextOutputFormat::class.java

    val outputPath = Path(args[1])

    if (args.size > 2) {
        try {
            job.numReduceTasks = Integer.parseInt(args[2])  // указываем количество reducer'ов
        } catch (e: NumberFormatException) {
            println("Указано неверное количество reducer'ов. Используем 1")
            job.numReduceTasks = 1
        }
    }

    // Configuring the input/output path from the filesystem into the job
    FileInputFormat.addInputPath(job, Path(args[0]))
    FileOutputFormat.setOutputPath(job, Path(args[1]))

    // deleting the output path automatically from hdfs so that we don't have delete it explicitly
    outputPath.getFileSystem(conf).delete(outputPath, true)

    // exiting the job only if the flag value becomes false
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
}
