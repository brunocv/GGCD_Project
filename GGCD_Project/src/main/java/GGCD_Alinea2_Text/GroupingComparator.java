package GGCD_Alinea2_Text;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//Class que controla quais as chaves que sao agrupadas juntas para uma unica chamada de Reducer.reduce()
public class GroupingComparator extends WritableComparator {

    //construtor por omissao
    protected GroupingComparator(){
        super(CompositeKeyWritable.class,true);
    }

    //funcao que controla quais as chaves que sao agrupadas juntas para uma unica chamada de Reducer.reduce()
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
        CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
        return key1.getYear().compareTo(key2.getYear());
    }

}
