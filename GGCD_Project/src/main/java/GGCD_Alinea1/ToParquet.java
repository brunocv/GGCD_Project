package GGCD_Alinea1;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

//Class que guarda toda a informacao, presente nos ficheiros, num unico ficheiro AvroParquet
public class ToParquet {

    //getSchema
    public static Schema getSchema() throws IOException {

        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream s = fs.open(new Path("hdfs:///schema.movies"));
        byte[] buf = new byte[10000];

        s.read(buf);

        String ps = new String(buf);
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);

    }

    //Mapper que trata do ficheiro title.basics.tsv.gz
    public static class ToParquetMapperLeft extends Mapper<LongWritable, Text, Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //passar a primeira linha
            if (key.get() == 0) return;

            String[] fields = value.toString().split("\t");

            StringBuilder values = new StringBuilder();

            //guardar tipo
            values.append(fields[1]);
            values.append("\t");

            //guardar primary title
            values.append(fields[2]);
            values.append("\t");

            //guardar titulo original
            values.append(fields[3]);
            values.append("\t");

            //guardar se e para adulto ou nao
            values.append(fields[4]);
            values.append("\t");

            //guardar ano inicial, se for \N fica a "null"
            if(!fields[5].equals("\\N")) values.append(fields[5]);
            else values.append("null");
            values.append("\t");

            //guardar ano final, se for \N fica a "null"
            if(!fields[6].equals("\\N")) values.append(fields[6]);
            else values.append("null");
            values.append("\t");

            //guardar minutos, se for \N fica a "null"
            if(!fields[7].equals("\\N")) values.append(fields[7]);
            else values.append("null");
            values.append("\t");

            //guardar genres, se nao tiver fica a null
            int i = 0;

            for(String s : fields[8].split(",")){
                if(i!=0) values.append(",");
                if(!s.equals("\\N"))values.append(s);
                else values.append("null");
                i++;
            }

            values.append("\t");

            context.write(new Text(fields[0]), new Text(values.toString()));
        }
    }

    //Mapper que trata do ficheiro title.ratings.tsv.gz
    public static class ToParquetMapperRight extends Mapper<LongWritable, Text, Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //passar a primeira linha
            if (key.get() == 0) return;

            String[] fields = value.toString().split("\t");

            //primeira sub-string: rating || segunda sub-string: votes
            StringBuilder values = new StringBuilder();

            //meter um R no inicio para depois no reducer saber se esta e a String (Text) de ratings ou nao, sendo que
            //queremos que esta String venha depois da String (Text) resultante do ToParquetMapperLeft
            values.append("R");
            values.append("\t");
            values.append(fields[1]);
            values.append("\t");
            values.append(fields[2]);

            context.write(new Text(fields[0]), new Text(values.toString()));
        }
    }

    //Cria GenericRecords com a juncao da informacao dos dois mappers
    public static class JoinReducer extends Reducer<Text,Text, Void, GenericRecord> {

        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            schema = getSchema();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            boolean has_basics = false;
            boolean has_ratings = false;

            GenericRecord record = new GenericData.Record(schema);

            //criadas 2 strings para manter a ordem que queremos
            String[] basics;
            String[] ratings;

            record.put("tconst",key);
            List<String> genres = new ArrayList<>();

            for(Text value : values){
                String[] aux = value.toString().split("\t");

                //se nao tiver R no inicio entao estamos perante o value de basics
                if(!aux[0].equals("R")){
                    basics = aux;
                    record.put("type",basics[0]);
                    record.put("primaryTitle",basics[1]);
                    record.put("originalTitle",basics[2]);
                    record.put("isAdult",basics[3]);
                    record.put("startYear",basics[4]);
                    record.put("endYear",basics[5]);
                    record.put("time",basics[6]);

                    String[] aux_gen = basics[7].split(",");
                    for(String s : aux_gen) genres.add(s);
                    record.put("genres",genres);

                    has_basics = true;
                }
                else if(aux[0].equals("R")){
                    ratings = aux;
                    record.put("rating", ratings[1]);
                    record.put("votes", ratings[2]);

                    has_ratings = true;
                }
            }

            //se tiver as duas entradas entao guardo, se so tiver basics crio ratings a nulo (porque se tem ja sei que e movie)
            //se so tiver ratings nao posso criar porque nao sei se era movie
            if(has_basics && has_ratings){
                context.write(null,record);
            }
            else if(has_basics && has_ratings == false){
                record.put("rating", "null");
                record.put("votes", "null");

                context.write(null,record);
            }
            else if(has_ratings && has_basics == false) return;
        }
    }

    //Main
    public static void main(String[] args) throws Exception{

        long startTime = System.nanoTime();

        Job job1 = Job.getInstance(new Configuration(), "ToParquetAlinea1");
        job1.setJarByClass(ToParquet.class);

        //input
        job1.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job1,new Path("hdfs:///title.basics.tsv.gz"),
                TextInputFormat.class, ToParquetMapperLeft.class);

        MultipleInputs.addInputPath(job1,new Path("hdfs:///title.ratings.tsv.gz"),
                TextInputFormat.class, ToParquetMapperRight.class);

        job1.setReducerClass(JoinReducer.class);

        //output
        job1.setOutputKeyClass(Void.class);
        job1.setOutputValueClass(GenericRecord.class);
        job1.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job1, getSchema());
        FileOutputFormat.setOutputPath(job1,new Path("hdfs:///resultado_parquet"));

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.waitForCompletion(true);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000; //miliseconds
        System.out.println("\n\nTIME: " + duration +"\n");
    }
}
