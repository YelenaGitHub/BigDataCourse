import mapper.LongestWordMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.LongestWordReducer;


/**
 * MapReduce program which takes text file as an input, searches the longest word and writes
 * this word to the output.
 * *
 * @author Elena Druzhilova
 * @since 4/11/2018
 */
public class LongestWord {

    private static final String JOB_NAME = "Longest word";

    /**
     * MapReduce setting for Configuration and Job.
     *
     *
     * @param args[0] - input file name or directory
     * @param args[1] - output
     * @param args[2] - count of the longest words need be retrieved
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // set as a parameter - the count of the longest word need be retrieved
        conf.set("LongestWordCount", args[2]);

        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(LongestWord.class);
        job.setMapperClass(LongestWordMapper.class);
        job.setCombinerClass(LongestWordReducer.class);
        job.setReducerClass(LongestWordReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
