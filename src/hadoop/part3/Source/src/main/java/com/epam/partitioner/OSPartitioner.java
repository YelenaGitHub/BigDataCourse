package com.epam.partitioner;

import com.epam.type.ImpressionCustomType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Objects;

/**
 * Partitioner divides data by city names.
 *
 * @author Elena Druzhilova
 * @since 4/13/2018
 */
public class OSPartitioner extends Partitioner<ImpressionCustomType, IntWritable> {

    /**
     * Divides city names into partitions depend on first character.
     *
     * @param text        cityName
     * @param intWritable count of bid price
     * @param i           number of Reducer tasks
     * @return
     */
    @Override
    public int getPartition(ImpressionCustomType text, IntWritable intWritable, int i) {
        int hashCode = Objects.hashCode(text.getOsType());
        return Math.abs(hashCode % i);
    }

}
