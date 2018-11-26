import combiner.AverageTotalBytesCombiner;
import mapper.IpByteMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import reducer.AverageTotalBytesReducer;
import type.CSVWritable;

/**
 * MapReduce program counts average bytes per request by IP and total bytes by IP.
 * *
 *
 * @author Elena Druzhilova
 * @since 4/12/2018
 */
public class LogAnalyzer {

    private static final String JOB_NAME = "Log Analyzer";

    /**
     * MapReduce settings for Configuration and Job.
     *
     * @param args[0] - input file name or directory
     * @param args[1] - output directory
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(LogAnalyzer.class);
        job.setMapperClass(IpByteMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CSVWritable.class);

        job.setCombinerClass(AverageTotalBytesCombiner.class);
        job.setReducerClass(AverageTotalBytesReducer.class);

        SequenceFileOutputFormat.setCompressOutput(job, true);
        // Snappy - не поддерживается под всеми, включая последнюю версию библиотек под Windows, в репозитории maven.
        // Вместо этого, пример c GZIP:
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);//SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        Counters counters = job.getCounters();

        System.out.println("Usage of Mozilla browser: " + counters.findCounter(IpByteMapper.COUNTERS.MOZILLA_USAGE_COUNT).getValue());
        System.out.println("Number of invalid records: " + counters.findCounter(IpByteMapper.COUNTERS.INVALID_RECORD_COUNT).getValue());

    }

}
