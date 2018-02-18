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
import org.apache.hadoop.util.ToolRunner
import java.io.File
import java.util.*


// Класс отвечает за подсчёт пар за первый месяц
class MapReduceWordCount : Configured(), Tool {
    // Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
    class Map : Mapper<LongWritable, Text, Text, IntWritable>() {
        override fun map(key: LongWritable, value: Text, context: Context) {
            val inputString = value.toString()

            // разделитель в строках .tsv-файла - табуляция
            val tokenizer = StringTokenizer(inputString, "\t")

            val firstWord = tokenizer.nextToken()
            val secondWord = tokenizer.nextToken()
            tokenizer.nextToken()
            val count = Integer.parseInt(tokenizer.nextToken())

            val wordPair = "$firstWord $secondWord"

            value.set(wordPair)
            context.write(value, IntWritable(count))  // выход mapper'а
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
        val job = Job.getInstance(conf, MapReduceWordCount::class.java.canonicalName)

        job.setJarByClass(MapReduceWordCount::class.java)
        job.mapperClass = MapReduceWordCount.Map::class.java
        job.combinerClass = MapReduceWordCount.Reduce::class.java
        job.reducerClass = MapReduceWordCount.Reduce::class.java

        job.outputKeyClass = Text::class.java
        job.outputValueClass = IntWritable::class.java

        val outputPath = Path(args[1])

        FileInputFormat.addInputPath(job, Path(args[0]))
        FileOutputFormat.setOutputPath(job, outputPath)

        // deleting the output path automatically from hdfs so that we don't have delete it explicitly
        outputPath.getFileSystem(conf).delete(outputPath, true)

        return if (job.waitForCompletion(true)) 0 else 1
    }
}

// Класс отвечает за подсчёт за подсчёт пар за второй месяц с учётом результатов топ пар в первый месяц
class MapReduceBroadcastJoin : Configured(), Tool {
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
        val job = Job.getInstance(conf, MapReduceBroadcastJoin::class.java.canonicalName)

        job.setJarByClass(MapReduceBroadcastJoin::class.java)
        job.mapperClass = MapReduceBroadcastJoin.Map::class.java
        job.combinerClass = MapReduceBroadcastJoin.Reduce::class.java
        job.reducerClass = MapReduceBroadcastJoin.Reduce::class.java

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

// пример вызова задачи:
// hadoop jar /home/n_chernetsov/Dropbox/Education/Otus_BigData/otus_BigData/HW14/MapReduceJoin/build/libs/MapReduce.jar /data/first_month /result/first_month WordCount
fun main(args: Array<String>) {
    if (args.size < 3) {
        println("Неправильное количество аргументов. Задайте: входную папку, выходную папку, тип задачи (WordCount, Join)")
    }

    val taskType = args[2]

    // в зависимости от типа задачи используем тот или иной класс
    when (taskType) {
        "WordCount" -> {
            val res = ToolRunner.run(MapReduceWordCount(), args)
            System.exit(res)
        }
        "Join" -> {
            val res = ToolRunner.run(MapReduceBroadcastJoin(), args)
            System.exit(res)
        }
        else -> { // Note the block
            println("Неправильный тип задачи. Используйте WordCount или Join")
        }
    }
}
