package GGCD_Alinea2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparatorText extends WritableComparator {

    protected GroupingComparatorText(){
        super(CompositeKeyWritableText.class,true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKeyWritableText key1 = (CompositeKeyWritableText) w1;
        CompositeKeyWritableText key2 = (CompositeKeyWritableText) w2;
        return key1.getYear().compareTo(key2.getYear());
    }

}
