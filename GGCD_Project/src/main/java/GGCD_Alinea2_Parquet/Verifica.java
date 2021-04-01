package GGCD_Alinea2_Parquet;

import GGCD_Alinea2_Text.CompositeKeyWritable;
import GGCD_Alinea2_Text.FromParquetToTextFile;
import GGCD_Alinea2_Text.GroupingComparator;
import GGCD_Alinea2_Text.PartitionerYear;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Verifica {
    //Recebe o ficheiro do esquema e fica com o Schema
    public static Schema getSchema(String schema) throws IOException {
        InputStream is = new FileInputStream(schema);
        String ps = new String(is.readAllBytes());
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    //Mapper para resolver a query 1, a cada entrada retorna key = ano e value = 1
    public static class FromParquetQueriesMapper extends Mapper<Void, GenericRecord, Text, Text> {

        private HashMap<String,String> query1 = new HashMap<>();
        private HashMap<String,String> query2 = new HashMap<>();

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            query1 = (HashMap<String, String>) value.get("query1");
            query2 = (HashMap<String, String>) value.get("query2");

            if(query1!=null){
                for (Map.Entry<String, String> entry : query1.entrySet()) {
                    String chave = entry.getKey() ;
                    context.write(new Text("query1" + "\t"+ chave), new Text(entry.getValue()));
                }
            }

            if(query2!=null){
                for (Map.Entry<String, String> entry : query2.entrySet()){
                    String chave = entry.getKey() ;
                    context.write(new Text("query2" + "\t"+ chave), new Text(entry.getValue()));
                }

            }
        }
    }

    public static class FromParquetQueriesReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text t : values) context.write(key,t);

        }
    }


    //Main
    public static void main(String args[]) throws Exception{

        // ########################## QUERY 1 #######################################

        Job job = Job.getInstance(new Configuration(),"FromParquetToTextFileQuery2Text");

        job.setJarByClass(Verifica.class);
        job.setMapperClass(FromParquetQueriesMapper.class);
        job.setReducerClass(FromParquetQueriesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job,new Path("resultado_alinea2"));
        Schema schema = getSchema("schema.alinea2");
        AvroParquetInputFormat.setRequestedProjection(job, schema);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("resultado_verifica"));
        job.waitForCompletion(true);

    }
}
