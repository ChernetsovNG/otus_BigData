import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import java.io.File
import java.util.*

// Класс отвечает за подсчёт за подсчёт пар за второй месяц с учётом результатов топ пар в первый месяц
class BroadcastJoin : Configured(), Tool {
    // Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
    class Map : Mapper<LongWritable, Text, Text, IntWritable>() {
        val firstMonthTopPairs = HashMap<String, Int>()

        override fun map(key: LongWritable, value: Text, context: Context) {
            val inputString = value.toString()

            // разделитель в строках .tsv-файла - табуляция
            val tokenizer = StringTokenizer(inputString, "\t")

            val firstWord = tokenizer.nextToken()
            val secondWord = tokenizer.nextToken()
            val type = tokenizer.nextToken()
            val count = Integer.parseInt(tokenizer.nextToken())

            val wordPair = "$firstWord $secondWord"

            // фильтруем второй месяц по тому, была ли эта пара в топе в первый месяц
            if (firstMonthTopPairs.containsKey(wordPair)) {
                value.set(wordPair)
                context.write(value, IntWritable(count))  // выход mapper'а
            }
        }

        @Override
        override fun setup(context: Context) {
            val cacheFiles = context.getCacheFiles()
            if (cacheFiles != null && cacheFiles.size > 0) {
                cacheFiles.forEach { readFile(Path(it.getPath())) }
            }
        }

        // заполняем карту топ 10000 пар из первого месяца
        private fun readFile(filePath: Path) {
            File(filePath.name.toString()).readLines().forEach {
                val stringTokenizer = StringTokenizer(it)
                val word1 = stringTokenizer.nextToken()
                val word2 = stringTokenizer.nextToken()
                val wordPair = "$word1 $word2"
                val count = Integer.parseInt(stringTokenizer.nextToken())
                firstMonthTopPairs[wordPair] = count
            }
        }
    }

    // Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
    class Reduce : Reducer<Text, IntWritable, Text, IntWritable>() {
        override fun reduce(key: Text, values: MutableIterable<IntWritable>, context: Context) {
            val sum = values.sumBy { it.get() }
            context.write(key, IntWritable(sum))
        }
    }

    @Override
    override fun run(args: Array<String>): Int {
        val job = Job.getInstance(conf, BroadcastJoin::class.java.canonicalName)

        job.setJarByClass(BroadcastJoin::class.java)
        job.mapperClass = BroadcastJoin.Map::class.java
        job.combinerClass = BroadcastJoin.Reduce::class.java
        job.reducerClass = BroadcastJoin.Reduce::class.java

        job.outputKeyClass = Text::class.java
        job.outputValueClass = IntWritable::class.java

        val outputPath = Path(args[1])

        FileInputFormat.addInputPath(job, Path(args[0]))
        FileOutputFormat.setOutputPath(job, outputPath)
        // 4-й аргумент - имя файла с результатами первого месяца
        job.addCacheFile(Path(args[3]).toUri())

        outputPath.getFileSystem(conf).delete(outputPath, true)

        return if (job.waitForCompletion(true)) 0 else 1
    }
}
