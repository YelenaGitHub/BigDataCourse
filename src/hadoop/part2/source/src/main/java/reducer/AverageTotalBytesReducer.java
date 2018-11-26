package reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import type.CSVWritable;

import java.io.IOException;

/**
 * Reducer collects data from custom CSVWritable types, get total bytes and calculates average bytes size.
 *
 * @author Elena Druzhilova
 * @since 4/12/2018
 */
public class AverageTotalBytesReducer extends Reducer<Text, CSVWritable, NullWritable, CSVWritable> {

    /**
     * Collects all incoming CSVWritable objects by IP and calculates average bytes per IP
     * Always get incoming data because of Mapper sends only records with bytes > 0.
     *
     * @param key     IP
     * @param values  List of CSVWritable types
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<CSVWritable> values, Context context) throws IOException, InterruptedException {

        int totalBytes = 0;
        int numberRequests = 0;
        float averageBytes = 0;

        CSVWritable writableResult = null;

        for (CSVWritable record : values) {
            totalBytes += record.getTotalBytes();
            numberRequests += record.getNumberRequests();
        }

        writableResult = new CSVWritable();
        writableResult.setIp(key);
        writableResult.setTotalBytes(totalBytes);
        averageBytes = totalBytes / numberRequests;
        writableResult.setAverageBytes(averageBytes);
        writableResult.setNumberRequests(numberRequests);

        // always get information from Combiner, therefore no extra conditions for checking incoming data that need to be omitted
        context.write(NullWritable.get(), writableResult);
    }

}
