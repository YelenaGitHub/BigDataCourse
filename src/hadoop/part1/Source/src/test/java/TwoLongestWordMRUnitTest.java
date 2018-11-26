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
 * @author Elena Druzhilova
 * @since 4/15/2018
 */
public class TwoLongestWordMRUnitTest {
    private Mapper mapper;
    private MapDriver mapDriver;

    private Reducer reducer;
    private ReduceDriver reduceDriver;

    private MapReduceDriver mapReduceDriver;

    private Text firstLine;
    private Text firstLine1MaxWord, firstLine2MaxWord;
    private Text secondLine;
    private Text secondLine1MaxWord, secondLine2MaxWord;
    private Text resultWord;

    private IntWritable firstLine1MaxWordLength, firstLine2MaxWordLength;
    private IntWritable secondLine1MaxWordLength, secondLine2MaxWordLength;
    private IntWritable resultWordLength;

    private String longestWordCount = "2";

    @Before
    public void setUp() {
        mapper = new LongestWordMapper();
        mapDriver = new MapDriver(mapper);

        reducer = new LongestWordReducer();
        reduceDriver = new ReduceDriver(reducer);

        mapReduceDriver = new MapReduceDriver(mapper, reducer);

        firstLine = new Text("preliminary filtering was applied on the map stage.");
        firstLine1MaxWord = new Text("preliminary");
        firstLine2MaxWord = new Text("filtering");
        firstLine1MaxWordLength = new IntWritable(11);
        firstLine2MaxWordLength = new IntWritable(9);

        secondLine = new Text("corresponding unit test is attached!");
        secondLine1MaxWord = new Text("corresponding");
        secondLine2MaxWord = new Text("attached");
        secondLine1MaxWordLength = new IntWritable(13);
        secondLine2MaxWordLength = new IntWritable(8);

        resultWord = secondLine1MaxWord;
        resultWordLength = secondLine1MaxWordLength;

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
                .withOutput(firstLine1MaxWordLength, firstLine1MaxWord)
                .withOutput(firstLine2MaxWordLength, firstLine2MaxWord)
                // TreeSet stores only maximum words, therefore at the 2nd line the mapper compares first and second lines tokens.
                .withOutput(secondLine1MaxWordLength, secondLine1MaxWord)
                .withOutput(firstLine1MaxWordLength, firstLine1MaxWord)
                .runTest(false);
    }

    @Test
    public void testLongestWordReducer() throws IOException {
        List<Text> firstLine1WordList = new ArrayList<>();
        firstLine1WordList.add(firstLine1MaxWord);

        List<Text> firstLine2WordList = new ArrayList<>();
        firstLine2WordList.add(firstLine2MaxWord);

        List<Text> secondLine1WordList = new ArrayList<>();
        secondLine1WordList.add(secondLine1MaxWord);

        List<Text> secondLine2WordList = new ArrayList<>();
        secondLine1WordList.add(secondLine2MaxWord);

        reduceDriver
                .withInput(firstLine1MaxWordLength, firstLine1WordList)
                .withInput(firstLine2MaxWordLength, firstLine2WordList)
                .withInput(secondLine1MaxWordLength, secondLine1WordList)
                .withInput(secondLine2MaxWordLength, secondLine2WordList)
                .withOutput(firstLine1MaxWordLength, firstLine1MaxWord)
                .withOutput(secondLine1MaxWordLength, secondLine1MaxWord)
                .runTest(false);
    }

    @Test
    public void testLongestWordMapReducer() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(0), firstLine)
                .withInput(new LongWritable(1), secondLine)
                .withOutput(firstLine1MaxWordLength, firstLine1MaxWord)
                .withOutput(secondLine1MaxWordLength, secondLine1MaxWord)
                .runTest(false);
    }

}
