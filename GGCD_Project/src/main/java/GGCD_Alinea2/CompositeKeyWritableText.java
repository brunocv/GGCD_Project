package GGCD_Alinea2;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyWritableText implements Writable, WritableComparable<CompositeKeyWritableText> {

    private String year;
    private String tconst;
    private String rating;

    public CompositeKeyWritableText() {}

    public CompositeKeyWritableText(String year, String tconst, String rating) {
        this.year = year;
        this.tconst = tconst;
        this.rating = rating;
    }

    @Override
    public String toString() {

        StringBuilder string = new StringBuilder();
        string.append(year);
        string.append("\t");
        string.append(tconst);
        string.append("\t");
        string.append(rating);

        return string.toString();
    }

    public void readFields(DataInput dataInput) throws IOException {
        year = WritableUtils.readString(dataInput);
        tconst = WritableUtils.readString(dataInput);
        rating = WritableUtils.readString(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, year);
        WritableUtils.writeString(dataOutput, tconst);
        WritableUtils.writeString(dataOutput, rating);
    }

    public int compareTo(CompositeKeyWritableText objKeyPair) {

        int result = year.compareTo(objKeyPair.year);
        if ( result == 0) {
            result = rating.compareTo(objKeyPair.rating);
        }
        return result;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getTconst() {
        return tconst;
    }

    public void setTconst(String tconst) {
        this.tconst = tconst;
    }

    public String getRating() {
        return rating;
    }

    public void setRating(String rating) {
        this.rating = rating;
    }
}
