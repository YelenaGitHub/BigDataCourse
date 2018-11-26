import com.epam.mapper.ImpressionMapper;
import com.epam.reducer.ImpressionReducer;
import com.epam.type.ImpressionCustomType;
import junit.framework.TestCase;
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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * MRUnit class for testing ImpressionAnalyzer's Mapper, Reducer and Mapper + Reducer as a whole logic.
 *
 * @author Elena Druzhilova
 * @since 4/13/2018
 */
public class ImpressionAnalyzerMRUnitTest extends TestCase {


    private Mapper mapper;
    private MapDriver mapDriver;

    private Reducer reducer;
    private ReduceDriver reduceDriver;

    private MapReduceDriver mapReduceDriver;

    private Text firstCity1Line, firstCity2Line, firstCity3Line, secondCityLine;
    private ImpressionCustomType firstCustomType, secondCustomType;
    private IntWritable firstCity1ImpressionCount, firstCity2ImpressionCount, firstCity3ImpressionCount, secondCityImpressionCount;
    private IntWritable bidPriceCount;

    private IntWritable resultFirstCityImpressionCount, resultSecondCityImpressionCount;
    private URI cityDistributedCache;

    @Before
    public void setUp() throws IOException {

        mapper = new ImpressionMapper();
        mapDriver = new MapDriver(mapper);

        reducer = new ImpressionReducer();
        reduceDriver = new ReduceDriver(reducer);

        mapReduceDriver = new MapReduceDriver(mapper, reducer);

        firstCity1Line = new Text("2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t1000\t48\tnull\t2259\t10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063");
        firstCity2Line = new Text("42285a786096878e506e07dbb24c1a15\t20131019162601169\t1\tD1EB6J0ifcu\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.94.80.*\t216\t234\t1\t418395ace7a0a45794d8074da47540f\t57976799a2b5ea945e8e3b78a37f3e58\tnull\tmm_11596315_1356062_9159970\t950\t90\tSecondView\tNa\t0\t7333\t200\t94\tnull\t2259\t16751,10059,10684,10079,10083,10102,10006,10148,10110,13403,10063,10116");
        firstCity3Line = new Text("42285a786096878e506e07dbb24c1a13\t20131019162601269\t1\tD1EB6J0ifcu\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.94.80.*\t216\t234\t1\t418395ace7a0a45794d8074da47540f\t57976799a2b5ea945e8e3b78a37f3e58\tnull\tmm_11596315_1356062_9159970\t950\t90\tSecondView\tNa\t0\t7333\t500\t94\tnull\t2259\t16751,10059,10684,10079,10083,10102,10006,10148,10110,13403,10063,10116");
        secondCityLine = new Text("c5d58ecb97b39746d088b2358f467533\t20131019151401123\t1\tD1RCOB26d0x\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30)\t61.142.131.*\t216\t200\t1\tcdc06a6ba07571f4b842a14390f861a\t5f36052d7957bc725b87a206447954c4\tnull\tmm_10029307_121417_10790029\t336\t280\tThirdView\tNa\t0\t7326\t300\t162\tnull\t2259\t10059,14273,10079,10075,13042,10083,10129,10024,10006,10110,13776,10031,10145,10127,10115,10063,11092");

        firstCity1ImpressionCount = new IntWritable(1000);
        firstCity2ImpressionCount = new IntWritable(200); // price < 250 should be omitted in Mapper
        firstCity3ImpressionCount = new IntWritable(500);
        secondCityImpressionCount = new IntWritable(300);

        firstCustomType = new ImpressionCustomType();
        firstCustomType.setCityName(new Text("zhongshan"));
        firstCustomType.setOsType(new Text("Windows XP"));

        secondCustomType = new ImpressionCustomType();
        secondCustomType.setCityName(new Text("shennongjia"));
        secondCustomType.setOsType(new Text("Windows XP"));

        bidPriceCount = new IntWritable(1);

        resultFirstCityImpressionCount = new IntWritable(2);
        resultSecondCityImpressionCount = new IntWritable(1);

        cityDistributedCache = URI.create("file:///C:/temp/city.en.txt");

        mapDriver.withCacheFile(cityDistributedCache);
        mapReduceDriver.withCacheFile(cityDistributedCache);
    }

    /**
     * Checks that bid price < 250 will be omitted in Mapper
     *
     * @throws IOException
     */
    @Test
    public void testImpressionAnalyzerMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(0), firstCity1Line)
                .withInput(new LongWritable(1), firstCity2Line)
                .withInput(new LongWritable(1), firstCity3Line)
                .withInput(new LongWritable(2), secondCityLine)
                .withOutput(firstCustomType, bidPriceCount)
                .withOutput(firstCustomType, bidPriceCount)
                .withOutput(secondCustomType, bidPriceCount)
                .runTest();
    }

    /**
     * Returns counts of bid price > 250
     *
     * @throws IOException
     */
    @Test
    public void testImpressionAnalyzerReducer() throws IOException {
        List<IntWritable> firstLineList = new ArrayList<>();
        firstLineList.add(bidPriceCount);
        firstLineList.add(bidPriceCount);

        List<IntWritable> secondLineList = new ArrayList<>();
        secondLineList.add(bidPriceCount);

        reduceDriver
                .withInput(firstCustomType, firstLineList)
                .withInput(secondCustomType, secondLineList)
                .withOutput(firstCustomType, resultFirstCityImpressionCount)
                .withOutput(secondCustomType, resultSecondCityImpressionCount)
                .runTest();
    }

    @Test
    public void testImpressionAnalyzer1CaseMapReducer() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(0), firstCity1Line)
                .withInput(new LongWritable(1), firstCity2Line)
                .withInput(new LongWritable(2), firstCity3Line)
                .withOutput(firstCustomType, resultFirstCityImpressionCount)
                .runTest();
    }

    @Test
    public void testImpressionAnalyzer2CaseMapReducer() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(3), secondCityLine)
                .withOutput(secondCustomType, resultSecondCityImpressionCount)
                .runTest();
    }


}
