package GGCD_Alinea2_Parquet;

import GGCD_Alinea2_Text.CompositeKeyWritable;
import GGCD_Alinea2_Text.FromParquetToTextFile;
import GGCD_Alinea2_Text.GroupingComparator;
import GGCD_Alinea2_Text.PartitionerYear;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

//Class para verificar o resultado de FromParquetToParquetFile
public class Verifica {

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

    //Mapper para resolver a query 1, a cada entrada retorna key = ano e value = 1
    public static class FromParquetQueriesMapper extends Mapper<Void, GenericRecord, Text, Text> {

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            String year = value.get("year").toString();
            String number_of_movies = value.get("number_of_movies").toString();
            String tconst_most_votes = value.get("tconst_most_votes").toString();
            String title_most_votes = value.get("title_most_votes").toString();
            String number_of_votes = value.get("number_of_votes").toString();

            List<String> top10 = new ArrayList<>();
            top10 = (List<String>)value.get("top10");

            StringBuilder entry = new StringBuilder();
            entry.append(number_of_movies);
            entry.append("\t");
            entry.append(tconst_most_votes);
            entry.append("\t");
            entry.append(title_most_votes);
            entry.append("\t");
            entry.append(number_of_votes);
            entry.append("\t\n");
            entry.append("Top 10: \n");
            int pos = 1;
            for(String s : top10){
                entry.append(pos + "- " + s + "\n");
                pos++;
            }

            context.write(new Text(year), new Text(entry.toString()));

        }
    }

    //Main
    public static void main(String args[]) throws Exception{

        long startTime = System.nanoTime();

        Job job = Job.getInstance(new Configuration(),"FromParquetToTextFileQuery2Text");

        job.setJarByClass(Verifica.class);
        job.setMapperClass(FromParquetQueriesMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job,new Path("hdfs:///resultado_alinea2"));
        Schema schema = getSchema("hdfs:///schema.alinea2");
        AvroParquetInputFormat.setRequestedProjection(job, schema);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs:///resultado_verifica"));
        job.waitForCompletion(true);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000; //miliseconds

        System.out.println("\n\nTIME: " + duration +"\n");

    }
}
