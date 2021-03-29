package GGCD_Alinea2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerText extends Partitioner<CompositeKeyWritableText, NullWritable> {

    @Override
    public int getPartition(CompositeKeyWritableText key, NullWritable value, int numReduceTasks){
        return (key.getYear().hashCode() % numReduceTasks);
    }
}
