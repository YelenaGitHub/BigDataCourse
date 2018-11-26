package com.epam.type;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom Type stores city name (just for learning Custom types).
 *
 * @author Elena Druzhilova
 * @since 4/15/2018
 */
@Getter
@Setter
public class ImpressionCustomType implements WritableComparable<ImpressionCustomType> {

    private Text cityName = new Text();
    private Text osType = new Text();

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        cityName.readFields(dataInput);
        osType.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        cityName.write(dataOutput);
        osType.write(dataOutput);
    }

    @Override
    public int compareTo(ImpressionCustomType o) {
        if ((this == o) || (o.equals(o.getCityName()) && cityName == o.getCityName() && osType == o.getOsType())) {
            return 0;
        } else {
            return osType.compareTo(o.getOsType());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImpressionCustomType that = (ImpressionCustomType) o;

        if (cityName != null ? !cityName.equals(that.cityName) : that.cityName != null) return false;
        return osType != null ? osType.equals(that.osType) : that.osType == null;
    }

    @Override
    public int hashCode() {
        int result = cityName != null ? cityName.hashCode() : 0;
        result = 31 * result + (osType != null ? osType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return cityName.toString() + "\t" + osType.toString();
    }

}
