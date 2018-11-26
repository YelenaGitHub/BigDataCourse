package com.epam;

import com.epam.combiner.ImpressionCombiner;
import com.epam.mapper.ImpressionMapper;
import com.epam.partitioner.OSPartitioner;
import com.epam.reducer.ImpressionReducer;
import com.epam.type.ImpressionCustomType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

/**
 * MapReduce program counts average bytes per request by IP and total bytes by IP.
 * *
 *
 * @author Elena Druzhilova
 * @since 4/13/2018
 */
public class ImpressionAnalyzer extends Configured implements Tool {

    private static final String JOB_NAME = "Impression Analyzer";
    /**
     * MapReduce settings for Configuration and Job.
     *
     * @param args[0] - input file name or directory
     * @param args[1] - output directory
     * @param args[2] - path to city file (distributed cache)
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int result = ToolRunner.run(conf, new ImpressionAnalyzer(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), JOB_NAME);
        job.setJarByClass(ImpressionAnalyzer.class);
        job.setMapperClass(ImpressionMapper.class);
        job.setMapOutputKeyClass(ImpressionCustomType.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(ImpressionCombiner.class);
        job.setPartitionerClass(OSPartitioner.class); // set custom partitioner

        job.setReducerClass(ImpressionReducer.class);

        job.setOutputKeyClass(ImpressionCustomType.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(7); // set number of reducers

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new File(args[2]).toURI());

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
