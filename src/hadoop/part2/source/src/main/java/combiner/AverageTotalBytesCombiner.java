package combiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import type.CSVWritable;

import java.io.IOException;

/**
 * Combiner counts total bytes and set number of Requests for further processing average bytes on the final Reducer.
 *
 * @author Elena Druzhilova
 * @since 4/12/2018
 */
public class AverageTotalBytesCombiner extends Reducer<Text, CSVWritable, Text, CSVWritable> {

    /**
     * Collects total bytes per IP and counts the number of requests for CSVWritable object.
     * Always get incoming data because of Mapper sends only records with bytes > 0.
     *
     * @param key     IP
     * @param values  List of CSVWritable objects
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<CSVWritable> values, Context context) throws IOException, InterruptedException {

        int totalBytes = 0;
        int numberRequests = 0;
        CSVWritable writableResult = null;

        for (CSVWritable bytes : values) {
            totalBytes += bytes.getTotalBytes();
            numberRequests++;
        }

        writableResult = new CSVWritable();

        writableResult.setIp(key);
        writableResult.setTotalBytes(totalBytes);
        writableResult.setNumberRequests(numberRequests);

        // always get information from Mapper, therefore no extra conditions for checking incoming data that need to be omitted
        context.write(key, writableResult);
    }

}
