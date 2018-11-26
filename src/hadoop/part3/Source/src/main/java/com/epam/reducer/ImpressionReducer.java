package com.epam.reducer;

import com.epam.type.ImpressionCustomType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer selects the total count of price by city name.
 *
 * @author Elena Druzhilova
 * @since 4/13/2018
 */
public class ImpressionReducer extends Reducer<ImpressionCustomType, IntWritable, ImpressionCustomType, IntWritable> {

    /**
     * Calculates amount of high-bid-priced  (more than 250) impression events by city.
     *
     * @param key     City name
     * @param values  Counts of bid price by a city with price more than 250
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(ImpressionCustomType key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int countOfMaxBidPrice = 0;

        for (IntWritable bidPriceCount: values) {
            countOfMaxBidPrice += bidPriceCount.get();
        }

        if (countOfMaxBidPrice > 0) {
            context.write(key, new IntWritable(countOfMaxBidPrice));
        }

    }
}
