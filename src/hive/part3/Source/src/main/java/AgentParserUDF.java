import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * AgentParserUDF is a custom Hive UDF presents logic for converting user agent string into multiple elements.
 *
 * @author Elena Druzhilova
 * @since 4/20/2018
 */

@Description(
        name = "agent_parser",
        value = "_FUNC_(arg1) - Converts arg1 to device, browser name, browser type and OS name",
        extended = "The function converts log Text to device, browser name, browser type and OS name."
)
@UDFType(deterministic = true, stateful = false)

public class AgentParserUDF extends UDF {

    /**
     * Parses agent log line into the list of elements.
     *
     * @param logLine - user agent log line
     * @return list of device type, browser name, browser type and OS name
     */
    public List<Text> evaluate(Text logLine) {
        if (logLine == null || logLine.toString().isEmpty()) {
            return null;
        }

        UserAgent userAgent = new UserAgent(logLine.toString());
        List<Text> fieldList = new ArrayList<>();

        fieldList.add(new Text(userAgent.getOperatingSystem().getDeviceType().getName()));
        fieldList.add(new Text(userAgent.getBrowser().getName()));
        fieldList.add(new Text(userAgent.getBrowser().getBrowserType().getName()));
        fieldList.add(new Text(userAgent.getOperatingSystem().getName()));

        return fieldList;
    }
}


