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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.checkerframework.checker.units.qual.C;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;


//Class que vai pegar no ficheiro AvroParquet criado pela class ToParquet e responder as queries
public class FromParquetToParquetFile{

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

    //Mapper para responder as queries
    public static class FromParquetQueriesMapper extends Mapper<Void, GenericRecord, Text, Text> {

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            if (!value.get("type").equals("movie")) return;

            String tconst = value.get("tconst").toString();
            String votes = value.get("votes").toString();
            String rating = value.get("rating").toString();

            if(!rating.equals("null") && !votes.equals("null"))
                context.write(new Text(value.get("startYear").toString()),
                        new Text(tconst +"\t" + value.get("originalTitle").toString() + "\t" + rating + "\t" +votes));
            else if(!rating.equals("null") && votes.equals("null"))
                context.write(new Text(value.get("startYear").toString()),
                        new Text(tconst +"\t" + value.get("originalTitle").toString() + "\t" + rating + "\t" + "-1"));
            else if(rating.equals("null") && !votes.equals("null"))
                context.write(new Text(value.get("startYear").toString()),
                        new Text(tconst +"\t" + value.get("originalTitle").toString() + "\t" + "-1" + "\t" + votes));
            else if(rating.equals("null") && votes.equals("null"))
                context.write(new Text(value.get("startYear").toString()),
                        new Text(tconst +"\t" + value.get("originalTitle").toString() + "\t" + "-1" + "\t" + "-1"));

        }
    }

    //Reducer para responder as queries
    public static class FromParquetQueriesReducer extends Reducer<Text, Text, Void, GenericRecord> {
        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            schema = getSchema("hdfs:///schema.alinea2");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long total_movies = 0;
            long most_votes = -1;
            int field_with_most_votes = -1;
            String tconst_most_votes = "";
            String title_most_votes = "";
            GenericRecord record = new GenericData.Record(schema);

            List<String> top10Year = new ArrayList<>();
            for(Text s : values){
                top10Year.add(s.toString());
                // s = tconst + \t + title + \t + rating + \t + votes

                String[] fields = s.toString().split("\t");
                total_movies++;

                field_with_most_votes = Integer.parseInt(fields[3]);
                if (field_with_most_votes >= most_votes) {
                    tconst_most_votes = fields[0];
                    title_most_votes = fields[1];
                    most_votes = field_with_most_votes;
                }
            }

            Collections.sort(top10Year, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    String[] aux = o1.split("\t");
                    String[] aux2 = o2.split("\t");

                    Double rating = Double.parseDouble(aux[2]);
                    Double rating2 = Double.parseDouble(aux2[2]);

                    int result = rating.compareTo(rating2);
                    if(result == 0){
                        Integer vote = Integer.parseInt(aux[3]);
                        Integer vote2 = Integer.parseInt(aux2[3]);
                        result = vote.compareTo(vote2);
                    }
                    return -result;
                }
            });

            List<String> result = new ArrayList<>();

            String ano = key.toString();
            int count = 0;

            for(String s : top10Year){
                //tconst title rating votes
                if(count == 10) break;

                String[] aux = s.split("\t");

                if(!aux[2].equals("-1")){
                    result.add(aux[0] + "\t" + aux[1] + "\t" + aux[2] + "\t" + aux[3]);
                    count++;
                }
            }

            record.put("year", key.toString());
            record.put("number_of_movies", total_movies);
            record.put("tconst_most_votes", tconst_most_votes);
            record.put("title_most_votes", title_most_votes);
            record.put("number_of_votes", most_votes);
            record.put("top10",result);
            context.write(null, record);
        }
    }

    //Main
    public static void main(String args[]) throws Exception {

        long startTime = System.nanoTime();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"FromParquetToTextFileAlinea2");

        job.setJarByClass(FromParquetToParquetFile.class);

        job.setMapperClass(FromParquetQueriesMapper.class);
        job.setReducerClass(FromParquetQueriesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job,new Path("hdfs:///resultado_parquet"));
        Schema queries = getSchema("hdfs:///schema.queries");
        Schema result = getSchema("hdfs:///schema.alinea2");
        AvroParquetInputFormat.setRequestedProjection(job, queries);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job, result);
        FileOutputFormat.setOutputPath(job,new Path("hdfs:///resultado_alinea2"));

        job.waitForCompletion(true);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000; //miliseconds

        System.out.println("\n\nTIME: " + duration +"\n");


    }

}

