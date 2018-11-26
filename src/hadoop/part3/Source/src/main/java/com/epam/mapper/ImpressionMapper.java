package com.epam.mapper;

import com.epam.type.ImpressionCustomType;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mapper class selects city and bid price from imp.*.txts files and
 * replace city id by city name.
 *
 * @author Elena Druzhilova
 * @since 4/13/2018
 */
public class ImpressionMapper extends Mapper<Object, Text, ImpressionCustomType, IntWritable> {

    private final int BID_PRICE_MAX = 250;
    private final IntWritable BID_PRICE_COUNT = new IntWritable(1);
    private final String TAB_SYMBOL = "\t";
    private final String DISTRIBUTED_CITY_FILE_NAME = "city.en.txt";
    private final String UNDEFINED_CITY_NAME_PROPERTY = "undefined";

    private Map<Integer, String> cities = new HashMap<>();

    /**
     * Collects city id and city name to HashMap from distributed cache file.
     *
     * @param uri URI of cities fileR
     * @throws Exception
     */
    private void readCites(URI uri) throws Exception {
        List<String> cityList = FileUtils.readLines(new File(uri));
        for (String cityRecord: cityList) {
            String[] cityLine = cityRecord.toString().split(TAB_SYMBOL);
            cities.put(Integer.valueOf(cityLine[0]), cityLine[1]);
        }
    }

    /**
     * Load cities' data cache.
     *
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            URI[] uris = context.getCacheFiles();
            for (URI uri : uris) {
                if (uri.toString().endsWith(DISTRIBUTED_CITY_FILE_NAME)) {
                    readCites(uri);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Can't find or open " + DISTRIBUTED_CITY_FILE_NAME);
        }
    }

    /**
     * Mapper produces city id as a key and bid price as a value when it more than 250.
     *
     * @param key     position in a file
     * @param value   line
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        int cityId = 0;
        int bidPrice = 0;
        UserAgent userAgent = null;

        try {
            String[] column = value.toString().split(TAB_SYMBOL);
            cityId = Integer.valueOf(column[7]);
            bidPrice = Integer.valueOf(column[19]);

            userAgent = new UserAgent(value.toString());

        } catch (Exception e) {
            return;
        }

        String cityName = cities.get(cityId);
        String osType = userAgent.getOperatingSystem().getName();

        if (cityName == null) {
            cityName = UNDEFINED_CITY_NAME_PROPERTY;
        }

        if (bidPrice > BID_PRICE_MAX) {
            ImpressionCustomType customType = new ImpressionCustomType();
            customType.setCityName(new Text(cityName));
            customType.setOsType(new Text(osType));
            context.write(customType, BID_PRICE_COUNT);
        }
    }

}
