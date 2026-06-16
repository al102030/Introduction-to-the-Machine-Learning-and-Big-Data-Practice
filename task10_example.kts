import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

/**
 * The word count example, which counts all words (read from a comma-separated-values (csv) file).
 *
 * The main method of the word count example initializing and submitting the WordCount job to hadoop.
 *
 * @param args the input and output path
 * @throws IOException
 * @throws InterruptedException
 * @throws ClassNotFoundException
 */
fun main(args: Array<String>) {

    val inputPath = Path(args[0])
    val outputDir = Path(args[1])

    // Create configuration
    val conf = Configuration(true)

    // Create job
    val job = Job.getInstance()
    job.setJarByClass(WordCountMapper::class.java)
	job.setJobName("WordCount")

    // Setup MapReduce
    job.setMapperClass(WordCountMapper::class.java)
    job.setReducerClass(WordCountReducer::class.java)
    job.setNumReduceTasks(1)

    // Specify key / value
    job.setOutputKeyClass(Text::class.java)
    job.setOutputValueClass(IntWritable::class.java)

    // Input
    FileInputFormat.addInputPath(job, inputPath)
    job.setInputFormatClass(TextInputFormat::class.java)

    // Output
    FileOutputFormat.setOutputPath(job, outputDir)
    job.setOutputFormatClass(TextOutputFormat::class.java)

    // Delete output if exists
    val hdfs = FileSystem.get(conf)
    if (hdfs.exists(outputDir))
        hdfs.delete(outputDir, true)

    // Execute job
    val code = if (job.waitForCompletion(true)) 0 else 1
    System.exit(code)

}

/**
 * The class for the hadoop mapper splitting the input into words and mapping them to word,1
 */
class WordCountMapper : Mapper<Any, Text, Text, IntWritable>() {

    private val ONE = IntWritable(1) // reusable object to save resources
    private val word = Text() // reusable object to save resources

    /**
     * The main map method mapping each word w to the key w with value 1
     */
    override fun map(key: Any, value: Text, context: Context) {

        // splitting the input according to the comma (We assume the words are separated by comma)
        val csv = value.toString().split(",")
        for (str in csv) {
            // for each word w write w,1 to the context (for the reduce phase)
            word.set(str)
            context.write(word, ONE)
        }
    }
}

/**
 * Reducer class to count the words, which were previously mapped to the words themselves with value 1
 */
class WordCountReducer : Reducer<Text, IntWritable, Text, IntWritable>() {

    /**
     * The reduce method, which just iterates through all occurrences of the same word, counts them and writes the word with the counted number
     */
    override fun reduce(text: Text, values: Iterable<IntWritable>, context: Context) {
        var sum = 0
        // count all occurrences of the current word
        for (value in values) {
            sum += value.get()
        }
        // write the result: the word plus the count of all its occurrences...
        context.write(text, IntWritable(sum))
    }
}