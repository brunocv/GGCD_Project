package GGCD_Alinea2_Text;

import org.apache.hadoop.io.NullWritable;

//Class que serve para indicar qual o mapper output que vai para cada reducer,
//de forma a que se tivermos mais que um reducer conseguimos garantir que X reducer ira ter as entradas todas
//de um certo ano (ou anos) para garantir que tem todas as entradas daquele ano e conseguir ordenar de forma fiavel
public class PartitionerYear extends org.apache.hadoop.mapreduce.Partitioner<CompositeKeyWritable, NullWritable> {

    //funcao que atraves do ano e do numero de reducers que se pediu, ira distribuir todas as entradas de um mesmo ano
    //para o mesmo reducer, ou seja, se tivermos 3 reducer e ano 2010,2011 e 2012 (por exemplo) serve para dizer que
    //reducer 1 fica com todas as entradas de 2010, reducer 2 fica com todas de 2011 etc e assim garantir que nenhuma entrada
    //de 2010 e mandada por exemplo para o reducer 2
    @Override
    public int getPartition(CompositeKeyWritable key, NullWritable value, int numReduceTasks){
        return (key.getYear().hashCode() % numReduceTasks);
    }
}
