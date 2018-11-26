package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import type.CSVWritable;
import type.LogWritable;

import java.io.IOException;

/**
 * Mapper class divides input log records into ip and Custom Writable class contains totals and average bytes.
 *
 * @author Elena Druzhilova
 * @since 4/12/2018
 */
public class IpByteMapper extends Mapper<Object, Text, Text, CSVWritable> {

    private static final String MOZILLA_BROWSER = "Mozilla";
    private static final long COUNTER_INCREMENT = 1l;
    private Text ip = new Text();
    private LogWritable logWritable = null;
    IntWritable totalBytes = new IntWritable(0);

    public enum COUNTERS {
        MOZILLA_USAGE_COUNT, INVALID_RECORD_COUNT;
    }

    /**
     * Parses incoming log record and tokenizes data into IP and custom CSVWritable type.
     * Collect information about invalid record counts and counts of usage Mozilla.
     *
     * @param key     IP
     * @param value   Bytes
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            logWritable = LogWritable.parseLog(value.toString());
            CSVWritable csvWritable = new CSVWritable();
            csvWritable.setIp(logWritable.getIp());
            totalBytes.set(Integer.parseInt(logWritable.getBytes().toString()));
            if (totalBytes.get() > 0) {
                csvWritable.setTotalBytes(Integer.parseInt(logWritable.getBytes().toString()));
                csvWritable.setAverageBytes(0);
                csvWritable.setNumberRequests(1);
                ip.set(logWritable.getIp());
                context.write(ip, csvWritable);
            }
        } catch (Exception e) {
            context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(COUNTER_INCREMENT);
        }

        if (logWritable != null && logWritable.getBrowser().toString().contains(MOZILLA_BROWSER)) {
            context.getCounter(COUNTERS.MOZILLA_USAGE_COUNT).increment(COUNTER_INCREMENT);
        }
    }

}
