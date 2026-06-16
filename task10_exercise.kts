import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

/**
 * One iteration of the pagerank algorithm
 */
val d = 0.85
val n = 4

/**
 * The main method of the pagerank example initializing and
 * submitting the PageRank job to hadoop.
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
    job.setJarByClass(PageRankMapper::class.java)
    job.setJobName("PageRank")
    // Setup MapReduce
    job.setMapperClass(PageRankMapper::class.java)
    job.setReducerClass(PageRankReducer::class.java)
    job.setNumReduceTasks(1)
    // Specify key / value
    job.setOutputKeyClass(Text::class.java)
    job.setOutputValueClass(DoubleWritable::class.java)
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
 * The class for the hadoop mapper splitting the input into Page, Rank
 * and outgoing edges
 */
class PageRankMapper : Mapper<Any, Text, Text, DoubleWritable>() {

    // reusable objects to save resources:
    private val targetPage = Text()
    private val outValue = DoubleWritable()

    /**
     * The main map method for mapping
     */
    override fun map(key: Any, value: Text, context: Context) {
        // splitting the input
        val pageRankEdges = value.toString().split(" ")
        val edges = pageRankEdges[2].toString().split(",".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()
        val rank = java.lang.Double.parseDouble(pageRankEdges[1])

// ========MY Sulotion======================================        
        val contr = rank / edges.size

        for (e in edges) {
            targetPage.set(e)
            outValue.set(contr)
            context.write(targetPage, outValue)
        }
//===========================================


    }
}


/**
 * Reducer class to calculate the new page rank of a page
 */
class PageRankReducer : Reducer<Text, DoubleWritable, Text, DoubleWritable>() {

    // reusable object to save resources:
    private val outValue = DoubleWritable()

    /**
     * The reduce method, which just iterates through all values summing
     * them up and calculating the new page rank
     */
    override fun reduce(text: Text, values: Iterable<DoubleWritable>, context: Context) {

// ===================== MY sulotion =====================

        var sum = 0.0
        for (value in values) {
            sum += value.get()
        }
        val newPageRank = ((1.0 - d) / n.toDouble()) + d * sum

      
      
        outValue.set(newPageRank)
        context.write(text, outValue)

// ============================================


    }
}