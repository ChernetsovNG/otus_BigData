package ru.nchernetsov;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class WordPairCount {
    // Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
    // файл предобработан так, что в каждой строке - отдельное предложение
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String inputSentence = value.toString();

            List<String> wordPairs = getWordPairs(inputSentence);

            for (String pair : wordPairs) {
                value.set(pair);
                context.write(value, new IntWritable(1));  // выход mapper'а
            }
        }

        // получить список пар слов в предложении
        private List<String> getWordPairs(String sentence) {
            StringTokenizer tokenizer = new StringTokenizer(sentence);
            List<String> sentenceTokens = new ArrayList<>();
            while (tokenizer.hasMoreTokens()) {
                sentenceTokens.add(tokenizer.nextToken());
            }
            // Очищаем слова от несловарных символов и повторяющихся пробелов
            List<String> clearSentenceTokens = sentenceTokens.stream()
                .map(this::clearToken).collect(Collectors.toList());
            clearSentenceTokens.remove("");

            // Составляем все возможные пары слов в предложении, разделённые пробелом
            return getListOfPairs(clearSentenceTokens);
        }

        private String clearToken(String token) {
            return token.toLowerCase()
                .replaceAll("[^a-zA-Zа-яА-Я0-9]", "")
                .replaceAll("[\\s]{2,}", " ");
        }

        private List<String> getListOfPairs(List<String> list) {
            List<String> listOfPairs = new ArrayList<>();
            for (int i = 0; i < list.size(); i++) {
                String word1 = list.get(i);
                for (int j = 0; j < list.size(); j++) {
                    String word2 = list.get(j);
                    if (i != j) {
                        listOfPairs.add(word1 + " " + word2);
                    }
                }
            }
            return listOfPairs;
        }
    }

    // Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // пример вызова задачи:
    // hadoop jar WordPairCount.jar /input /output
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordPairCount");

        job.setJarByClass(WordPairCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputPath = new Path(args[1]);

        // Configuring the input/output path from the filesystem into the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // deleting the output path automatically from hdfs so that we don't have delete it explicitly
        outputPath.getFileSystem(conf).delete(outputPath, true);

        // exiting the job only if the flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
