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
import java.util.*

// Класс отвечает за подсчёт пар за первый месяц
class WordCount : Configured(), Tool {
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
        val job = Job.getInstance(conf, WordCount::class.java.canonicalName)

        job.setJarByClass(WordCount::class.java)
        job.mapperClass = WordCount.Map::class.java
        job.combinerClass = WordCount.Reduce::class.java
        job.reducerClass = WordCount.Reduce::class.java

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
