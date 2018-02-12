import PrePostProcessing.getSentenceList
import PrePostProcessing.getWordPairs
import java.io.File
import java.util.*
import java.util.regex.Pattern
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

fun main(args: Array<String>) {
    // preprocess()
    postprocess()
}

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

fun postprocess() {
    val pathToResultFolder = "/home/n_chernetsov/Dropbox/Education/Otus_BigData/otus_BigData/HW13/result"
    val resultFile1 = pathToResultFolder + "/part-r-00000"
    val resultFile2 = pathToResultFolder + "/part-r-00001"
    val resultFile3 = pathToResultFolder + "/part-r-00002"
    val pathToOutputFile = pathToResultFolder + "/top-pairs"

    val pairs1 = getWordPairs(resultFile1)
    val pairs2 = getWordPairs(resultFile2)
    val pairs3 = getWordPairs(resultFile3)

    val allPairs = HashMap<String, Int>()
    allPairs.putAll(pairs1)
    allPairs.putAll(pairs2)
    allPairs.putAll(pairs3)

    println("All pairs count: " + allPairs.size)

    // Карта вида <Кол-во раз -> список пар>
    val countPairsMap = HashMap<Int, ArrayList<String>>()

    for ((pair, count) in allPairs) {
        if (!countPairsMap.contains(count)) {
            countPairsMap.put(count, ArrayList())
        }
        val list = countPairsMap[count]
        list?.add(pair)
    }

    val result: ArrayList<Pair<Int, List<String>>> = ArrayList()
    for ((count, listOfPairs) in countPairsMap.toSortedMap()) {
        result.add(Pair(count, listOfPairs))
    }

    val resultByDesc = result.reversed()  // сверху будут пары, которых больше всего

    // записываем предложения построчно в текстовый файл
    File(pathToOutputFile).printWriter().use { out ->
        resultByDesc.forEach {
            out.println("${it.first} : ${it.second.take(25)}")  // запишем только 10 первых пар
        }
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
        val lines = File(inputFile).readLines()
        for (line in lines) {
            val tokenizer = StringTokenizer(line)
            if (tokenizer.countTokens() == 3) {  // если у нас пара слов и число
                val word1 = tokenizer.nextToken()
                val word2 = tokenizer.nextToken()
                val wordPair = "$word1 $word2"
                val pairCount = Integer.parseInt(tokenizer.nextToken())
                result.put(wordPair, pairCount)
            }
        }
        return result
    }
}