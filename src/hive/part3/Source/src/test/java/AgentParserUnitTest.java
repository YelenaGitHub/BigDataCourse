import junit.framework.TestCase;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Elena Druzhilova
 * @since 4/18/2018
 */
public class AgentParserUnitTest extends TestCase {

    @Test
    public void testValidStringAgentParserUDF() {
        Text logLine = new Text("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; QQBrowser/7.4.14018.400)");
        Text deviceType = new Text("Computer");
        Text browserName = new Text("Internet Explorer 8");
        Text browserType = new Text("Browser");
        Text osName = new Text("Windows XP");

        AgentParserUDF agentParserUDF2 = new AgentParserUDF();
        List<Text> resultFieldList = agentParserUDF2.evaluate(logLine);

        List<Text> columnList = new ArrayList<>();
        columnList.add(deviceType);
        columnList.add(browserName);
        columnList.add(browserType);
        columnList.add(osName);

        Assert.assertEquals(resultFieldList, columnList);
    }

    @Test
    public void testNotValidStringAgentParserUDF() {
        Text logLine = new Text("10059,10077,10075,10083,10006,10111,10131,10126,13403,10063,10116");

        Text unknownResult = new Text("Unknown");
        Text unknownResultLower = new Text("unknown");

        Text deviceTYpe = unknownResult;
        Text browserName = unknownResult;
        Text browserType = unknownResultLower;
        Text osName = unknownResult;

        AgentParserUDF agentParserUDF2 = new AgentParserUDF();
        List<Text> resultFieldList = agentParserUDF2.evaluate(logLine);

        List<Text> columnList = new ArrayList<>();

        columnList.add(deviceTYpe);
        columnList.add(browserName);
        columnList.add(browserType);
        columnList.add(osName);

        Assert.assertEquals(resultFieldList, columnList);
    }
}
