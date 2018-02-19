import org.apache.hadoop.util.ToolRunner

// пример вызова задачи:
// hadoop jar /home/n_chernetsov/Dropbox/Education/Otus_BigData/otus_BigData/HW14/MapReduceJoin/build/libs/MapReduce.jar /data/first_month /result/first_month WordCount
fun main(args: Array<String>) {
    if (args.size < 3) {
        println("Неправильное количество аргументов. Задайте: входную папку, выходную папку, тип задачи (WordCount, BroadcastJoin, ReduceJoin)")
    }

    val taskType = args[2]

    // в зависимости от типа задачи используем тот или иной класс
    val returnCode: Int
    when (taskType) {
        TaskType.WORD_COUNT.taskType -> {
            returnCode = ToolRunner.run(WordCount(), args)
        }
        TaskType.BROADCAST_JOIN.taskType -> {
            returnCode = ToolRunner.run(BroadcastJoin(), args)
        }
        TaskType.REDUCE_JOIN.taskType -> {
            returnCode = ToolRunner.run(ReduceJoin(), args)
        }
        else -> { // Note the block
            println("Неправильный тип задачи. Используйте WordCount, BroadcastJoin или ReduceJoin")
            returnCode = 0
        }
    }
    System.exit(returnCode)
}

enum class TaskType(val taskType: String) {
    WORD_COUNT("WordCount"),
    BROADCAST_JOIN("BroadcastJoin"),
    REDUCE_JOIN("ReduceJoin")
}
