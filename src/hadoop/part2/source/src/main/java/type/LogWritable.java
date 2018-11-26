package type;

import eu.bitwalker.useragentutils.UserAgent;
import lombok.Getter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Custom type for parsing input record data.
 * Defines browser information via UserAgent third party lib.
 *
 * @author Elena Druzhilova
 * @since 4/12/2018
 */
@Getter
public class LogWritable implements WritableComparable<LogWritable> {

    private Text ip, browser, bytes;
    private static String LOG_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
    private static Pattern pattern = Pattern.compile(LOG_PATTERN);

    private LogWritable(Text ip, Text browser, Text bytes) {
        this.ip = ip;
        this.browser = browser;
        this.bytes = bytes;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ip.readFields(dataInput);
        browser.readFields(dataInput);
        bytes.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ip.write(dataOutput);
        browser.write(dataOutput);
        bytes.write(dataOutput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogWritable that = (LogWritable) o;

        if (!ip.equals(that.ip)) return false;
        if (browser != null ? !browser.equals(that.browser) : that.browser != null) return false;
        return bytes != null ? bytes.equals(that.bytes) : that.bytes == null;
    }

    @Override
    public int hashCode() {
        int result = ip.hashCode();
        result = 31 * result + (browser != null ? browser.hashCode() : 0);
        result = 31 * result + (bytes != null ? bytes.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(LogWritable o) {
        if ((this == o) || (ip.equals(o.getIp()) && browser.equals(o.getBrowser()) && bytes.equals(o.getBytes()))) {
            return 0;
        } else {
            return ip.compareTo(o.getIp());
        }
    }

    public static LogWritable parseLog(String logLine) {
        Matcher matcher = pattern.matcher(logLine);

        if (!matcher.find()) {
            throw new RuntimeException("Cannot parse line " + logLine);
        }

        UserAgent userAgent = new UserAgent(logLine);

        return new LogWritable(new Text (matcher.group(1)),
                new Text (userAgent.getBrowser().getName()),
                new Text (matcher.group(7)));

    }

}
