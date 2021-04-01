package GGCD_Alinea3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//Class que controla quais as chaves que sao agrupadas juntas para uma unica chamada de Reducer.reduce()
public class GroupingComparatorGenre extends WritableComparator {

    //construtor por omissao
    protected GroupingComparatorGenre(){
        super(CompositeKeyWritableA3.class,true);
    }

    //funcao que controla quais as chaves que sao agrupadas juntas para uma unica chamada de Reducer.reduce()
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKeyWritableA3 key1 = (CompositeKeyWritableA3) w1;
        CompositeKeyWritableA3 key2 = (CompositeKeyWritableA3) w2;
        return key1.getGenero().compareTo(key2.getGenero());
    }

}
