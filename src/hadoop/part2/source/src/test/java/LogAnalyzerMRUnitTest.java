import junit.framework.TestCase;

import mapper.IpByteMapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import reducer.AverageTotalBytesReducer;
import type.CSVWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MRUnit class for testing LogAnalyzer's Mapper, Reducer and Mapper + Reducer as a whole logic.
 *
 * @author Elena Druzhilova
 * @since 4/12/2018
 */
public class LogAnalyzerMRUnitTest extends TestCase {

    private Mapper mapper;
    private MapDriver mapDriver;

    private Reducer reducer;
    private ReduceDriver reduceDriver;

    private MapReduceDriver mapReduceDriver;

    private Text firstLine;
    private Text secondLine;
    private Text thirdLine;

    private Text firstIP, secondIP;

    private float averageBytes;
    private int numberRequests;

    private CSVWritable csvWritable1, csvWritable2, csvWritable3;
    private CSVWritable csvWritableResult1, csvWritableResult2;

    @Before
    public void setUp() {

        mapper = new IpByteMapper();
        mapDriver = new MapDriver(mapper);

        reducer = new AverageTotalBytesReducer();
        reduceDriver = new ReduceDriver(reducer);

        mapReduceDriver = new MapReduceDriver(mapper, reducer);

        firstLine = new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ss5_jumpers.jpg HTTP/1.1\" 200 100 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"");
        secondLine = new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ss5_rear.jpg HTTP/1.1\" 200 200 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"");
        thirdLine = new Text("ip15 - - [24/Apr/2011:04:47:46 -0400] \"GET /images/sun_logo.gif HTTP/1.1\" 200 500 \"http://www.google.com/search?um=1&hl=en&safe=off&client=firefox-a&rls=org.mozilla%3Aen-US%3Aofficial&biw=1920&bih=862&site=search&tbs=isz%3Ai&tbm=isch&sa=1&q=sun&aq=f&aqi=g10&aql=&oq=\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"");

        firstIP = new Text("ip2");
        secondIP = new Text("ip15");

        averageBytes = 0;
        numberRequests = 1;

        csvWritable1 = new CSVWritable();
        csvWritable1.setIp(firstIP);
        csvWritable1.setTotalBytes(100);
        csvWritable1.setAverageBytes(averageBytes);
        csvWritable1.setNumberRequests(numberRequests);

        csvWritable2 = new CSVWritable();
        csvWritable2.setIp(firstIP);
        csvWritable2.setTotalBytes(200);
        csvWritable2.setAverageBytes(averageBytes);
        csvWritable2.setNumberRequests(numberRequests);

        csvWritable3 = new CSVWritable();
        csvWritable3.setIp(secondIP);
        csvWritable3.setTotalBytes(500);
        csvWritable3.setAverageBytes(averageBytes);
        csvWritable3.setNumberRequests(numberRequests);

        csvWritableResult1 = new CSVWritable();
        csvWritableResult1.setIp(firstIP);
        csvWritableResult1.setTotalBytes(300);
        csvWritableResult1.setAverageBytes(150);
        csvWritableResult1.setNumberRequests(2);

        csvWritableResult2 = new CSVWritable();
        csvWritableResult2.setIp(secondIP);
        csvWritableResult2.setTotalBytes(500);
        csvWritableResult2.setAverageBytes(500);
        csvWritableResult2.setNumberRequests(1);
    }

    @Test
    public void testLogAnalyzerMapper() throws IOException {
        mapDriver
            .withInput(new LongWritable(0), firstLine)
            .withInput(new LongWritable(1), secondLine)
            .withInput(new LongWritable(2), thirdLine)
            .withOutput(firstIP, csvWritable1)
            .withOutput(firstIP, csvWritable2)
            .withOutput(secondIP, csvWritable3)
            .runTest();
    }

    @Test
    public void testLogAnalyzerReducer() throws IOException {
        List<CSVWritable> firstLineList = new ArrayList<>();
        firstLineList.add(csvWritable1);
        firstLineList.add(csvWritable2);

        List<CSVWritable> secondLineList = new ArrayList<>();
        secondLineList.add(csvWritable3);

        reduceDriver
            .withInput(firstIP, firstLineList)
            .withInput(secondIP, secondLineList)
            .withOutput(NullWritable.get(), csvWritableResult1)
            .withOutput(NullWritable.get(), csvWritableResult2)
            .runTest();
    }

    @Test
    public void testLogAnalyzerReducer1Case() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(0), firstLine)
                .withInput(new LongWritable(1), secondLine)
                .withOutput(NullWritable.get(), csvWritableResult1)
                .runTest();
    }

    @Test
    public void testLogAnalyzerMapReducer2Case() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(2), thirdLine)
                .withOutput(NullWritable.get(), csvWritableResult2)
                .runTest();
    }


}
