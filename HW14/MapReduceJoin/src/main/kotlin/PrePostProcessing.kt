import PrePostProcessing.getSentenceList
import PrePostProcessing.getWordPairs
import java.io.File
import java.io.FileInputStream
import java.util.*
import java.util.regex.Pattern
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

fun main(args: Array<String>) {
    // preprocess()
    // postprocess()
    // writePartMonthTopPairsInFiles()
    // get10000topPairs()
    // addTabulationTo10000TopPairs()
}

const val pathToResultFolder = "/home/n_chernetsov/Dropbox/Education/Otus_BigData/otus_BigData/HW14/result"
const val firstMonthResultFile = pathToResultFolder + "/part-r-00000"
const val pathToFirstMonthResultFolder = pathToResultFolder + "/first_month_top_pairs/"

fun preprocess() {
    val pathToInputFile = "/home/n_chernetsov/Dropbox/Education/Otus_BigData/otus_BigData/HW13/alice30.txt"
    val pathToOutputFile = "/home/n_chernetsov/Dropbox/Education/Otus_BigData/otus_BigData/HW13/alice30_prep.txt"

    val text = File(pathToInputFile).readText()

    val sentences = getSentenceList(text)

    // записываем предложения построчно в текстовый файл
    File(pathToOutputFile).printWriter().use { out ->
        sentences.forEach {
            out.println(it)
        }
    }
}

fun writePartMonthTopPairsInFiles() {
    // т.к. входной файл очень большой, то будем его обрабатывать построчно,
    // записывая результаты по 10 млн. строк
    var count = 0L
    var fileNumber = 1
    val partMap = HashMap<String, Int>()

    FileInputStream(firstMonthResultFile).use {
        Scanner(it, "UTF-8").use {
            while (it.hasNextLine()) {
                val line = it.nextLine()
                val tokenizer = StringTokenizer(line, "\t")
                if (tokenizer.countTokens() == 2) {  // если у нас пара слов и число
                    val wordPair = tokenizer.nextToken()
                    val pairCount = Integer.parseInt(tokenizer.nextToken())
                    partMap.put(wordPair, pairCount)
                    count++
                }
                if (count % 10_000_000 == 0L) {
                    println("Read $count lines from input file")
                    val partResult = partMap.toList().sortedByDescending { (_, value) -> value }.take(10_000)
                    val pathToOutputFile = pathToFirstMonthResultFolder + "part_result_${fileNumber}.txt"
                    File(pathToOutputFile).printWriter().use { out ->
                        partResult.forEach {
                            out.println("${it.first} ${it.second}")
                        }
                    }
                    partMap.clear()
                    fileNumber++
                }
            }
            if (it.ioException() != null) {
                throw it.ioException()
            }
        }
    }

    // записываем всё, что осталось
    val partResult = partMap.toList().sortedByDescending { (_, value) -> value }.take(10_000)
    val pathToOutputFile = pathToFirstMonthResultFolder + "part_result_${fileNumber}.txt"
    File(pathToOutputFile).printWriter().use { out ->
        partResult.forEach {
            out.println("${it.first} ${it.second}")
        }
    }
    partMap.clear()

    println("Write $fileNumber files. Read $count strings from input file")
}

fun get10000topPairs() {
    val partMap = HashMap<String, Int>()

    // Считываем файлы с частичными резульлтатами (по 1000 топ пар из 10 млн. исходных строк)
    val folder = File(pathToFirstMonthResultFolder)
    for (file in folder.listFiles()) {
        file.readLines().forEach {
            val tokenizer = StringTokenizer(it)
            val word1 = tokenizer.nextToken()
            val word2 = tokenizer.nextToken()
            val wordPair = "$word1 $word2"
            val pairCount = Integer.parseInt(tokenizer.nextToken())
            partMap.put(wordPair, pairCount)
        }
    }

    // Отбираем 10000 топ пар
    val result = partMap.toList().sortedByDescending { (_, value) -> value }.take(10_000)

    File(pathToResultFolder + "/first_month_top_pairs.txt").printWriter().use { out ->
        result.forEach {
            out.println("${it.first} ${it.second}")
        }
    }
}

// Вставить в качестве разделителя между словами и кол-вом в топ-парах за первый месяц табуляцию
fun addTabulationTo10000TopPairs() {
    val lines = File(pathToResultFolder + "/first_month_top_pairs.txt").readLines()

    File(pathToResultFolder + "/first_month_top_pairs_tab.txt").printWriter().use { out ->
        lines.forEach {
            val line = it
            val stringTokenizer = StringTokenizer(line)
            val word1 = stringTokenizer.nextToken()
            val word2 = stringTokenizer.nextToken()
            val count = stringTokenizer.nextToken()
            val lineWithTab = "$word1 $word2\t$count"  // вставляем в качестве разделителя табуляцию
            out.println(lineWithTab)
        }
    }
}

fun postprocess() {
    val pathToResultFolder = "/home/n_chernetsov/Dropbox/Education/Otus_BigData/otus_BigData/HW14/result"
    val resultFile1 = pathToResultFolder + "/part-r-00000"
    val pathToOutputFile = pathToResultFolder + "/first_month_top_pairs"

    val pairs1 = getWordPairs(resultFile1)

    val allPairs = HashMap<String, Int>()
    allPairs.putAll(pairs1)

    println("All pairs count: " + allPairs.size)

    // выбираем первые 10000 пар
    val result = allPairs.toList().sortedByDescending { (_, value) -> value }.toMap()


    // записываем предложения построчно в текстовый файл
    File(pathToOutputFile).printWriter().use { out ->
        // do nothing
    }
}

// Предобработка входного текста и постобработка результатов Map-Reduce
object PrePostProcessing {
    // Разбиваем текст на список предложений
    fun getSentenceList(inputText: String): ArrayList<String> {
        var result = ArrayList<String>()
        val matcher = Pattern.compile("([^.!?]+)").matcher(inputText)
        while (matcher.find()) {
            val sentence = matcher.group(1).toLowerCase()
            result.add(sentence)
        }
        result = result.map { it -> it.replace("\n", "") } as ArrayList<String>
        return result
    }

    // Получить пары и их количества из заданного файла
    fun getWordPairs(inputFile: String): Map<String, Int> {
        val result = HashMap<String, Int>()

        val bufferedReader = File(inputFile).bufferedReader()
        bufferedReader.useLines { lines ->
            lines.forEach {
                val tokenizer = StringTokenizer(it, "\t")
                if (tokenizer.countTokens() == 2) {  // если у нас пара слов и число
                    val wordPair = tokenizer.nextToken()
                    val pairCount = Integer.parseInt(tokenizer.nextToken())
                    result.put(wordPair, pairCount)
                }
                if (result.size % 10_000_000 == 0) {
                    println("Read ${result.size} lines from input file")
                }
            }
        }

        return result
    }
}
