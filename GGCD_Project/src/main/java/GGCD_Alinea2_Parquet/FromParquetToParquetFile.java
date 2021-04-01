package GGCD_Alinea2_Parquet;

import GGCD_Alinea3.CompositeKeyWritableA3;
import GGCD_Alinea3.FromParquetToTextAlinea3;
import GGCD_Alinea3.GroupingComparatorGenre;
import net.minidev.json.writer.CollectionMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


//Class que vai pegar no ficheiro AvroParquet criado pela class AvroParquet e responder as queries
//Os resultados serao guardados em ficheiros de texto separado
//Usado para verificacao de resultados
public class FromParquetToParquetFile{

    public static HashMap<String, String> query1 = new HashMap<>();
    public static HashMap<String, String> query2 = new HashMap<>();

    //Recebe o ficheiro do esquema e fica com o Schema
    public static Schema getSchema(String schema) throws IOException {
        InputStream is = new FileInputStream(schema);
        String ps = new String(is.readAllBytes());
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    //Mapper para responder as queries
    public static class FromParquetQueriesMapper extends Mapper<Void, GenericRecord, Text, Text> {

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            if (!value.get("type").equals("movie")) return;

            String tconst = value.get("tconst").toString();

            context.write(new Text("query1\t" + value.get("startYear").toString()), new Text("1"));

            if(!value.get("votes").equals("null"))
                context.write(new Text("query2\t" + value.get("startYear").toString()),new Text(tconst +"\t" + value.get("originalTitle").toString() + "\t" + value.get("votes").toString()));
            else context.write(new Text("query2\t" + value.get("startYear").toString()),new Text(tconst +"\t" + value.get("originalTitle").toString() + "\t" + "-1"));

        }
    }

    //Combiner para responder as queries
    public static class FromParquetQueriesCombiner extends Reducer<Text,Text, Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] query = key.toString().split("\t");

            if(query[0].equals("query1")){
                long total = 0;
                for(Text value : values){
                    total++;
                }
                if(query1.containsKey(query[1])){
                    long aux = Long.parseLong(query1.get(query[1]));
                    total += aux;
                    query1.put(query[1],String.valueOf(total));
                }
                else query1.put(query[1], String.valueOf(total));

                context.write(new Text("done"),new Text(""));
            }
            else if(query[0].equals("query2")){
                long maior = -1;
                String tconst = "";
                String title = "";

                for(Text value : values){
                    String[] fields = value.toString().split("\t");
                    if(Integer.parseInt(fields[2]) >= maior){
                        tconst = fields[0];
                        title = fields[1];
                        maior = Integer.parseInt(fields[2]);
                    }
                }
                if(query2.containsKey(query[1])){
                    String[] aux = query2.get(query[1]).split("\t");
                    if(Long.parseLong(aux[2]) < maior){
                        query2.put(query[1], tconst + "\t" + title + "\t" + maior);
                    }
                }
                else query2.put(query[1], tconst + "\t" + title + "\t" + maior);

                context.write(new Text("done"),new Text(""));
            }
        }
    }

    //Reducer para responder as queries
    public static class FromParquetQueriesReducer extends Reducer<Text, Text, Void, GenericRecord> {
        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            schema = getSchema("schema.alinea2");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            GenericRecord record = new GenericData.Record(schema);

            if(key.toString().equals("done")){
                record.put("query1",query1);
                record.put("query2",query2);
            }

            context.write(null, record);
        }
    }

    //Main
    public static void main(String args[]) throws Exception {
        Job job = Job.getInstance(new Configuration(),"FromParquetToTextFileAlinea2");

        job.setJarByClass(FromParquetToParquetFile.class);

        job.setMapperClass(FromParquetQueriesMapper.class);
        job.setCombinerClass(FromParquetQueriesCombiner.class);
        job.setReducerClass(FromParquetQueriesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job,new Path("resultado_parquet"));
        Schema queries = getSchema("schema.queries");
        Schema result = getSchema("schema.alinea2");
        AvroParquetInputFormat.setRequestedProjection(job, queries);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job, result);
        FileOutputFormat.setOutputPath(job,new Path("resultado_alinea2"));

        job.waitForCompletion(true);

    }

}

