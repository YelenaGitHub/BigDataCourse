import junit.framework.TestCase;

import mapper.LongestWordMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import reducer.LongestWordReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MRUnit class for testing LongestWord's Mapper, Reducer and Mapper + Reducer as a whole logic.
 *
 * @author Elena Druzhilova
 * @since 4/11/2018
 */
public class OneLongestWordMRUnitTest extends TestCase {

    private Mapper mapper;
    private MapDriver mapDriver;

    private Reducer reducer;
    private ReduceDriver reduceDriver;

    private MapReduceDriver mapReduceDriver;

    private Text firstLine;
    private Text firstLineMaxWord;
    private Text secondLine;
    private Text secondLineMaxWord;
    private Text resultWord;

    private IntWritable firstLineMaxWordLength;
    private IntWritable secondLineMaxWordLength;
    private IntWritable resultWordLength;

    private String longestWordCount = "1";

    @Before
    public void setUp() {
        mapper = new LongestWordMapper();
        mapDriver = new MapDriver(mapper);

        reducer = new LongestWordReducer();
        reduceDriver = new ReduceDriver(reducer);

        mapReduceDriver = new MapReduceDriver(mapper, reducer);

        firstLine = new Text("preliminary filtering was applied on the map stage.");
        firstLineMaxWord = new Text("preliminary");
        firstLineMaxWordLength = new IntWritable(11);

        secondLine = new Text("corresponding unit test is attached!");
        secondLineMaxWord = new Text("corresponding");
        secondLineMaxWordLength = new IntWritable(13);

        resultWord = secondLineMaxWord;
        resultWordLength = secondLineMaxWordLength;

        // set count of the longest words count need to be retrieved
        mapDriver.getConfiguration().set("LongestWordCount", longestWordCount);
        reduceDriver.getConfiguration().set("LongestWordCount", longestWordCount);
        mapReduceDriver.getConfiguration().set("LongestWordCount", longestWordCount);
    }

    @Test
    public void testLongestWordMapper() throws IOException {
        mapDriver
            .withInput(new LongWritable(0), firstLine)
            .withInput(new LongWritable(1), secondLine)
            .withOutput(firstLineMaxWordLength, firstLineMaxWord)
            .withOutput(secondLineMaxWordLength, secondLineMaxWord)
            .runTest();
    }

    @Test
    public void testLongestWordReducer() throws IOException {
        List<Text> firstLineList = new ArrayList<>();
        firstLineList.add(firstLineMaxWord);

        List<Text> secondLineList = new ArrayList<>();
        secondLineList.add(secondLineMaxWord);

        reduceDriver
            .withInput(firstLineMaxWordLength, firstLineList)
            .withInput(secondLineMaxWordLength, secondLineList)
            .withOutput(secondLineMaxWordLength, secondLineMaxWord)
            .runTest();
    }

    @Test
    public void testLongestWordMapReducer() throws IOException {
        mapReduceDriver
            .withInput(firstLineMaxWordLength, firstLine)
            .withInput(secondLineMaxWordLength, secondLine)
            .withOutput(resultWordLength, resultWord)
            .runTest();
    }

}
