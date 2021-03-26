package GGCD_Alinea1e2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ToParquet {

    public static Schema getSchema() throws IOException {
        InputStream is = new FileInputStream("schema.movies");
        String ps = new String(is.readAllBytes());
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    //Mapper que trata do ficheiro title.basics.tsv.bz2
    public static class ToParquetMapperLeft extends Mapper<LongWritable, Text, Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //passar a primeira linha
            if (key.get() == 0) return;

            String[] fields = value.toString().split("\t");

            //se não for filme não guardamos
            if(!fields[1].equals("movie")) return;


            //indice 0: title || indice 1: year || indice 2: genres
            StringBuilder values = new StringBuilder();

            values.append(fields[3] + "##");


            if(!fields[5].equals("\\N")) values.append(fields[5] + "##");
            else values.append("null##");

            for(String s : fields[8].split(",")){
                if(!s.equals("\\N"))values.append(s + "##");
                else values.append("null##");
            }

            context.write(new Text(fields[0]), new Text(values.toString()));
        }
    }

    //Mapper que trata do ficheiro title.ratings.tsv.bz2
    public static class ToParquetMapperRight extends Mapper<LongWritable, Text, Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //passar a primeira linha
            if (key.get() == 0) return;

            String[] fields = value.toString().split("\t");


            //indice 0: rating || indice 1: votes
            StringBuilder values = new StringBuilder();

            values.append("," + fields[1] + "##");
            values.append(fields[2] + "##");

            context.write(new Text(fields[0]), new Text(values.toString()));
        }
    }


    //Juntar a informacao dos 2 Mappers para um ficheiro (funciona)
    //public static class JoinReducer2 extends Reducer<Text,Text, Text, Text>
    /*
    public static class JoinReducer2 extends Reducer<Text,Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int i = 0;

            //criadas 2 strings para manter a ordem do esquema que queremos
            String aux = "";
            String aux2 = "";

            for(Text value : values){
                if(value.toString().charAt(0) == ',' ) aux += value.toString();
                else aux2 += value.toString();
                i++;
            }

            if(i <= 1) return;
            context.write(key,new Text(aux2 + aux));
        }
    }
    */

    public static class JoinReducer extends Reducer<Text,Text, Void, GenericRecord> {

        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            schema = getSchema();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int i = 0;

            //criadas 2 strings para manter a ordem do esquema que queremos
            String aux = "";
            String aux2 = "";

            for(Text value : values){
                if(value.toString().charAt(0) == ',' ) aux2 += value.toString();
                else aux += value.toString();
                i++;
            }
            if(i <= 1) return;

            GenericRecord record = new GenericData.Record(schema);
            record.put("tconst",key);
            String[] values1 = aux.toString().split("##");
            String[] values2 = aux2.toString().split("##");

            record.put("title", values1[0]);
            record.put("year", values1[1]);
            record.put("rating", values2[0]);
            record.put("votes", values2[1]);

            List<String> l = new ArrayList<>();

            for(int j = 2; j < values1.length ; j++){
                l.add(values1[j]);
            }
            record.put("genres", l);
            context.write(null,record);
        }
    }

    public static void main(String[] args) throws Exception{

        //1st file: title.basics.tsv.bz2
        //2nd file: title.ratings.tsv.bz2

        Job job1 = Job.getInstance(new Configuration(), "ToParquetAlinea1");
        job1.setJarByClass(ToParquet.class);

        //input
        job1.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job1,new Path("/home/bruno/Desktop/GGCD/Dados/teste/title.basics.tsv.bz2"),
                TextInputFormat.class, ToParquetMapperLeft.class);

        MultipleInputs.addInputPath(job1,new Path("/home/bruno/Desktop/GGCD/Dados/teste/title.ratings.tsv.bz2"),
                TextInputFormat.class, ToParquetMapperRight.class);

        job1.setReducerClass(JoinReducer.class);

        //output

        /*
        //Para guardar em ficheiro (texto) usar este em vez do de baixo e mudar o reducer para o JoinReducer2
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1,new Path("resultado"));
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        */

        job1.setOutputKeyClass(Void.class);
        job1.setOutputValueClass(GenericRecord.class);
        job1.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job1, getSchema());
        FileOutputFormat.setOutputPath(job1,new Path("resultado"));


        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.waitForCompletion(true);
    }
}
