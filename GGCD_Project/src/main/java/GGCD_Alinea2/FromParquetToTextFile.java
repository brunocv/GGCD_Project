package GGCD_Alinea2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;


//Class que vai pegar no ficheiro AvroParquet criado pela class AvroParquet e responder as queries
//Os resultados serao guardados em ficheiros de texto separado
//Usado para verificacao de resultados
public class FromParquetToTextFile {

    //Recebe o ficheiro do esquema e fica com o Schema
    public static Schema getSchemaQuery(String schema) throws IOException {
        InputStream is = new FileInputStream(schema);
        String ps = new String(is.readAllBytes());
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    //Mapper para resolver a query 1, a cada entrada retorna key = ano e value = 1
    public static class FromParquetQuery1Mapper extends Mapper<Void, GenericRecord, Text, LongWritable> {

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            if(!value.get("type").equals("movie")) return;

            context.write(new Text(value.get("startYear").toString()),new LongWritable(1));

        }
    }

    //Reducer para resolver a query 1, junta todos as keys iguais e faz o somatorios dos values
    public static class FromParqueQuery1Reducer extends Reducer<Text,LongWritable, Text,LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            for(LongWritable value : values){
                total += value.get();
            }
            context.write(key,new LongWritable(total));
        }
    }

    //Mapper para resolver a query 2, a cada entrada retorna key = ano e value = tconst + votos
    public static class FromParquetQuery2Mapper extends Mapper<Void, GenericRecord, Text, Text> {

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            if(!value.get("type").equals("movie")) return;

            String tconst = value.get("tconst").toString();
            //Como guardamos todas as entradas de basics no parquet e se não estiver no ratings metemos os votos a null,
            //temos de dizer que se tiver votos a null passa a -1
            //cada context tera: ano -> (key) + (tconst + votes) -> value
            if(!value.get("votes").equals("null"))
                context.write(new Text(value.get("startYear").toString()),new Text(tconst +"\t" + value.get("votes").toString()));
            else context.write(new Text(value.get("startYear").toString()),new Text(tconst +"\t" + "-1"));

        }
    }

    //Reducer para resolver a query 2, junta todos as keys iguais e ve qual dos filmes desse ano tem mais votos
    public static class FromParqueQuery2Reducer extends Reducer<Text,Text, Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long maior = -1;
            String tconst = "";

            for(Text value : values){
                String[] fields = value.toString().split("\t");
                if(Integer.parseInt(fields[1]) >= maior){
                    tconst = fields[0];
                    maior = Integer.parseInt(fields[1]);
                }
            }
            context.write(key,new Text(tconst + "\t" + maior));
        }
    }

    //Mapper para resolver a query 3, a cada entrada retorna key = ano e value = tconst + rating
    public static class FromParquetQuery3Mapper extends Mapper<Void, GenericRecord, CompositeKeyWritableText, NullWritable> {

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            if(!value.get("type").equals("movie")) return;

            String tconst = value.get("tconst").toString();

            //Como guardamos todas as entradas de basics no parquet e se não estiver no ratings metemos os ratings a null,
            //temos de dizer que se tiver rating a null passa a -1
            //cada context tera: ano -> (key) + (tconst + rating) -> value

            if(!value.get("rating").equals("null")){
                CompositeKeyWritableText newKey = new CompositeKeyWritableText(value.get("startYear").toString(),tconst,value.get("rating").toString());
                context.write(newKey, NullWritable.get());
            }
            else{
                CompositeKeyWritableText newKey = new CompositeKeyWritableText(value.get("startYear").toString(),tconst,"-1.0");
                context.write(newKey, NullWritable.get());
            }
        }
    }

    //Reducer para resolver a query 3, junta todos as keys iguais e ve qual dos filmes desse ano tem mais votos
    public static class FromParqueQuery3Reducer extends Reducer<CompositeKeyWritableText,NullWritable, CompositeKeyWritableText,NullWritable> {

        @Override
        protected void reduce(CompositeKeyWritableText key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            int i = 0;
            for (NullWritable value : values) {
                if(i == 10) break;
                context.write(key, NullWritable.get());
                i++;
            }

        }
    }

    //Main
    public static void main(String args[]) throws Exception{

        // ########################## QUERY 1 #######################################

        long startTime = System.nanoTime();

        Job job_query1 = Job.getInstance(new Configuration(),"FromParquetToTextFileQuery1");

        job_query1.setJarByClass(FromParquetToTextFile.class);
        job_query1.setMapperClass(FromParquetQuery1Mapper.class);
        job_query1.setReducerClass(FromParqueQuery1Reducer.class);

        job_query1.setOutputKeyClass(Text.class);
        job_query1.setOutputValueClass(LongWritable.class);

        job_query1.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job_query1,new Path("resultado_parquet"));
        Schema schema1;
        schema1 = getSchemaQuery("schema.query1");
        AvroParquetInputFormat.setRequestedProjection(job_query1, schema1);

        job_query1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job_query1,new Path("resultado_from_parquet_query1"));

        job_query1.waitForCompletion(true);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000; //miliseconds
        System.out.println("\n\nTIME: " + duration +"\n");

        // ########################## QUERY 2 #######################################

        startTime = System.nanoTime();

        Job job_query2 = Job.getInstance(new Configuration(),"FromParquetToTextFileQuery2");

        job_query2.setJarByClass(FromParquetToTextFile.class);
        job_query2.setMapperClass(FromParquetQuery2Mapper.class);
        job_query2.setReducerClass(FromParqueQuery2Reducer.class);

        job_query2.setOutputKeyClass(Text.class);
        job_query2.setOutputValueClass(Text.class);

        job_query2.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job_query2,new Path("resultado_parquet"));
        Schema schema2;
        schema2 = getSchemaQuery("schema.query2");
        AvroParquetInputFormat.setRequestedProjection(job_query2, schema2);

        job_query2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job_query2,new Path("resultado_from_parquet_query2"));

        job_query2.waitForCompletion(true);

        endTime = System.nanoTime();
        duration = (endTime - startTime)/1000000; //miliseconds
        System.out.println("\n\nTIME: " + duration +"\n");

        // ########################## QUERY 3 #######################################

        startTime = System.nanoTime();

        Job job_query3 = Job.getInstance(new Configuration(),"FromParquetToTextFileQuery3");

        job_query3.setJarByClass(FromParquetToTextFile.class);

        job_query3.setMapperClass(FromParquetQuery3Mapper.class);
        job_query3.setMapOutputKeyClass(CompositeKeyWritableText.class);
        job_query3.setMapOutputValueClass(NullWritable.class);
        job_query3.setPartitionerClass(PartitionerText.class);
        job_query3.setSortComparatorClass(KeySortComparatorText.class);
        job_query3.setGroupingComparatorClass(GroupingComparatorText.class);
        job_query3.setReducerClass(FromParqueQuery3Reducer.class);
        job_query3.setOutputKeyClass(CompositeKeyWritableText.class);
        job_query3.setOutputValueClass(NullWritable.class);
        //job_query3.setNumReduceTasks(8);

        job_query3.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job_query3,new Path("resultado_parquet"));
        Schema schema3;
        schema3 = getSchemaQuery("schema.query3");
        AvroParquetInputFormat.setRequestedProjection(job_query3, schema3);

        job_query3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job_query3,new Path("resultado_from_parquet_query3"));

        job_query3.waitForCompletion(true);

        endTime = System.nanoTime();
        duration = (endTime - startTime)/1000000; //miliseconds
        System.out.println("\n\nTIME: " + duration +"\n");
    }
}
