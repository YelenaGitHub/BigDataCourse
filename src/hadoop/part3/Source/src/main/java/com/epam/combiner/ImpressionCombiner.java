package com.epam.combiner;

import com.epam.type.ImpressionCustomType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner calculates the count of bid prices that more than 250
 *
 * @author Elena Druzhilova
 * @since 4/13/2018
 */
public class ImpressionCombiner extends Reducer<ImpressionCustomType, IntWritable, ImpressionCustomType, IntWritable> {

    /**
     * Total count of bid prices than more than 250 by key
     *
     * @param key     city name
     * @param values  count of bid prices > 250
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(ImpressionCustomType key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int countOfMaxBidPrice = 0;

        for (IntWritable bidPriceCount: values) {
            countOfMaxBidPrice++;
        }

        context.write(key, new IntWritable(countOfMaxBidPrice));
    }
}
