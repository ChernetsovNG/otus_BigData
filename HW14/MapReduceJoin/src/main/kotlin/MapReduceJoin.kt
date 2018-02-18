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

class MapReduceJoin {
    // Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
    class Map : Mapper<LongWritable, Text, Text, IntWritable>() {
        override fun map(key: LongWritable, value: Text, context: Context) {
            val inputString = value.toString()

            // разделитель в строках .tsv-файла - табуляция
            val tokenizer = StringTokenizer(inputString, "\t")

            val firstWord = tokenizer.nextToken()
            val secondWord = tokenizer.nextToken()
            val type = tokenizer.nextToken()
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
}

// пример вызова задачи:
// hadoop jar /home/n_chernetsov/Dropbox/Education/Otus_BigData/otus_BigData/HW14/MapReduceJoin/build/libs/MapReduceJoin.jar /data/first_month /result/first_month
fun main(args: Array<String>) {
    val conf = Configuration()
    val job = Job.getInstance(conf, "MapReduceJoin")

    job.setJarByClass(MapReduceJoin::class.java)
    job.mapperClass = MapReduceJoin.Map::class.java
    job.reducerClass = MapReduceJoin.Reduce::class.java

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
