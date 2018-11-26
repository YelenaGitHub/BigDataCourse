package type;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom type for generating results in CSV format as ip,averageBytes,totalBytes.
 *
 * @author Elena Druzhilova
 * @since 4/12/2018
 */
@Getter
@Setter
public class CSVWritable implements WritableComparable<CSVWritable> {

    private Text ip = new Text();
    private int totalBytes;
    private float averageBytes;
    private int numberRequests;

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ip.readFields(dataInput);
        totalBytes = dataInput.readInt();
        averageBytes = dataInput.readFloat();
        numberRequests = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ip.write(dataOutput);
        dataOutput.writeInt(totalBytes);
        dataOutput.writeFloat(averageBytes);
        dataOutput.writeInt(numberRequests);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CSVWritable writable = (CSVWritable) o;

        if (totalBytes != writable.totalBytes) return false;
        if (Float.compare(writable.averageBytes, averageBytes) != 0) return false;
        if (numberRequests != writable.numberRequests) return false;
        return ip != null ? ip.equals(writable.ip) : writable.ip == null;
    }

    @Override
    public int hashCode() {
        int result = ip != null ? ip.hashCode() : 0;
        result = 31 * result + totalBytes;
        result = 31 * result + (averageBytes != +0.0f ? Float.floatToIntBits(averageBytes) : 0);
        result = 31 * result + numberRequests;
        return result;
    }

    @Override
    public int compareTo(CSVWritable o) {
        if ((this == o) || (ip.equals(o.getIp()) && totalBytes == o.getTotalBytes() && averageBytes == o.getAverageBytes())) {
            return 0;
        } else {
            return ip.compareTo(o.getIp());
        }
    }

    @Override
    public String toString() {
        return ip + "," + averageBytes + "," + totalBytes;
    }

}
