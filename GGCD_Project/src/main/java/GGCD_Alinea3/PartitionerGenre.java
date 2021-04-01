package GGCD_Alinea3;

import org.apache.hadoop.io.NullWritable;

//Class que serve para indicar qual o mapper output que vai para cada reducer,
//de forma a que se tivermos mais que um reducer conseguimos garantir que X reducer ira ter as entradas todas
//de um certo genero (ou generos) para garantir que tem todas as entradas daquele genero e conseguir ordenar de forma fiavel
public class PartitionerGenre extends org.apache.hadoop.mapreduce.Partitioner<CompositeKeyWritableA3, NullWritable> {

    //funcao que atraves do genero e do numero de reducers que se pediu, ira distribuir todas as entradas de um mesmo genero para o mesmo reducer
    @Override
    public int getPartition(CompositeKeyWritableA3 key, NullWritable value, int numReduceTasks){
        return (key.getGenero().hashCode() % numReduceTasks);
    }
}
