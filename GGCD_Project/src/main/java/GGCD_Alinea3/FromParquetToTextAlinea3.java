package GGCD_Alinea3;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


//Class que vai pegar no ficheiro AvroParquet criado pela class ToParquet e responder a alinea 3
//Os resultados serao guardados em ficheiros de texto separado
public class FromParquetToTextAlinea3 {

    //Recebe o ficheiro do esquema e fica com o Schema
    public static Schema getSchema(String schema) throws IOException {

        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream s = fs.open(new Path(schema));
        byte[] buf = new byte[10000];

        s.read(buf);

        String ps = new String(buf);
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    //Mapper para resolver a alinea 3, a cada entrada retorna key = CompositeKeyWritable (esta tem secondary sort) e value = NullWritable
    public static class FromParquetAlinea3Mapper extends Mapper<Void, GenericRecord, CompositeKeyWritableA3, NullWritable> {

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            if(!value.get("type").equals("movie")) return;

            String genre = "";
            List<String> genres = new ArrayList<>();

            genres = (List<String>) value.get("genres");

            for(String s : genres){
                if(s.equals("null")) return;
                genre = s;
                break;
            }

            String tconst = value.get("tconst").toString();
            String originalTitle = value.get("originalTitle").toString();
            String rating = value.get("rating").toString();
            String votes = value.get("votes").toString();


            if(!rating.equals("null") && !votes.equals("null")){
                CompositeKeyWritableA3 newKey = new CompositeKeyWritableA3(tconst,originalTitle,rating,votes,genre);
                context.write(newKey, NullWritable.get());
            }
            else if(!rating.equals("null") && votes.equals("null")){
                CompositeKeyWritableA3 newKey = new CompositeKeyWritableA3(tconst,originalTitle,rating,"-1",genre);
                context.write(newKey, NullWritable.get());
            }
            else if(rating.equals("null")) return;

        }
    }

    //Reducer para resolver a alinea 3, junta todas as keys com o mesmo genero e fica com o top 2 de rating para cada genero (quando entra no reduce ja vem ordenado)
    public static class FromParquetAlinea3Reducer extends Reducer<CompositeKeyWritableA3,NullWritable, CompositeKeyWritableA3,NullWritable> {

        @Override
        protected void reduce(CompositeKeyWritableA3 key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            int i = 0;
            for (NullWritable value : values) {
                if(i == 2) break;
                context.write(key, NullWritable.get());
                i++;
            }

        }
    }

    //Mapper que atraves do schema vai ler os campos desejados e atribuir o filme do mesmo genero com melhor classificacao
    public static class FromParquetFinalMapper extends Mapper<Void, GenericRecord, Text,Text> {

        HashMap<String,String> generos = new HashMap<>();

        //funcao que vai carregar os dados da fase 1 com o top 2 rating para cada genero para memoria
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] mapsideFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(new Configuration());

            BufferedReader reader;

            for(URI u : mapsideFiles){

                try {
                    FSDataInputStream s = fs.open(new Path(u.getPath()));

                    reader = new BufferedReader( new InputStreamReader(s));

                    int i = 0;
                    String line;
                    int j = 0;
                    String[] auxLine;
                    while ((line = reader.readLine()) != null) {

                        auxLine = line.split("\t");
                        if(j==2) j = 0;
                        generos.put(auxLine[0]+j,auxLine[1] + "\t" + auxLine[2] + "\t" +auxLine[3] + "\t" + auxLine[4]);
                        j++;
                        i++;

                    }

                    System.out.println("Numero de linhas de top 2 generos: " + i);
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            if(!value.get("type").equals("movie")) return;
            String genre = "";
            List<String> genres = new ArrayList<>();

            genres = (List<String>) value.get("genres");

            for(String s : genres){
                if(s.equals("null")) return;
                genre = s;
                break;
            }

            String tconst = value.get("tconst").toString();
            String originalTitle = value.get("originalTitle").toString();
            String rating = value.get("rating").toString();
            String votes = value.get("votes").toString();

            if(generos.containsKey(genre+"0")) {
                String[] aux = generos.get(genre + "0").split("\t");
                if (!aux[0].equals(tconst)) {
                    StringBuilder toReturn = new StringBuilder();
                    toReturn.append(tconst);
                    toReturn.append("\t");
                    toReturn.append(originalTitle);
                    toReturn.append("\t");
                    toReturn.append(genre);
                    toReturn.append("\t");
                    toReturn.append(rating);
                    toReturn.append("\t");
                    toReturn.append(votes);
                    toReturn.append("\t");
                    toReturn.append("RECOMMENDED:");
                    toReturn.append("\t");
                    toReturn.append(generos.get(genre + "0"));

                    context.write(new Text(tconst),new Text(toReturn.toString()));
                }
                else {
                    if (generos.containsKey(genre + "1")) {
                        aux = generos.get(genre + "1").split("\t");
                        if (!aux[0].equals(tconst)) {
                            StringBuilder toReturn = new StringBuilder();
                            toReturn.append(tconst);
                            toReturn.append("\t");
                            toReturn.append(originalTitle);
                            toReturn.append("\t");
                            toReturn.append(genre);
                            toReturn.append("\t");
                            toReturn.append(rating);
                            toReturn.append("\t");
                            toReturn.append(votes);
                            toReturn.append("\t");
                            toReturn.append("RECOMMENDED:");
                            toReturn.append("\t");
                            toReturn.append(generos.get(genre + "1"));
                            context.write(new Text(tconst),new Text(toReturn.toString()));
                        }
                    }
                }
            }
            return;

        }

    }

    //Reducer que so serve para escrever no ficheiro sem este ser dividido
    public static class FromParquetAlinea3FinalReducer extends Reducer<Text,Text, NullWritable,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text t : values){
                context.write(NullWritable.get(),t);
            }
        }
    }

    //Main
    public static void main(String args[]) throws Exception{

        // ########################## ALINEA 3 FASE 1 #######################################

        long startTime = System.nanoTime();

        Job job_1 = Job.getInstance(new Configuration(),"FromParquetToTextFileAlinea3Fase1");

        job_1.setJarByClass(FromParquetToTextAlinea3.class);

        job_1.setMapperClass(FromParquetAlinea3Mapper.class);
        job_1.setMapOutputKeyClass(CompositeKeyWritableA3.class);
        job_1.setMapOutputValueClass(NullWritable.class);
        job_1.setGroupingComparatorClass(GroupingComparatorGenre.class); //agrupar por generos
        job_1.setReducerClass(FromParquetAlinea3Reducer.class);
        job_1.setOutputKeyClass(CompositeKeyWritableA3.class);
        job_1.setOutputValueClass(NullWritable.class);

        job_1.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job_1,new Path("hdfs:///resultado_parquet"));
        Schema schema;
        schema = getSchema("hdfs:///schema.alinea3");
        AvroParquetInputFormat.setRequestedProjection(job_1, schema);

        job_1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job_1,new Path("hdfs:///resultado_from_parquet_alinea3_fase1"));

        job_1.waitForCompletion(true);

        // ########################## ALINEA 3 FASE 2 #######################################

        Job job_2 = Job.getInstance(new Configuration(),"FromParquetToTextFileAlinea3Fase2");

        job_2.setJarByClass(FromParquetToTextAlinea3.class);
        job_2.setMapperClass(FromParquetFinalMapper.class);
        job_2.setReducerClass(FromParquetAlinea3FinalReducer.class);

        job_2.addCacheFile(URI.create("hdfs:///resultado_from_parquet_alinea3_fase1/part-r-00000"));

        job_2.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job_2,new Path("hdfs:///resultado_parquet"));
        AvroParquetInputFormat.setRequestedProjection(job_2, schema);

        job_2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job_2,new Path("hdfs:///resultado_from_parquet_alinea3_fase2"));

        job_2.setMapOutputKeyClass(Text.class);
        job_2.setMapOutputValueClass(Text.class);
        job_2.setOutputKeyClass(NullWritable.class);
        job_2.setOutputValueClass(Text.class);
        job_2.waitForCompletion(true);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000; //miliseconds
        System.out.println("\n\nTIME: " + duration +"\n");
    }
}
