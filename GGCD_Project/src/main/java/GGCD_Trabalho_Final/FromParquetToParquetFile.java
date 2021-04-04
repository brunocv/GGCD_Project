package GGCD_Trabalho_Final;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


//Class que vai pegar no ficheiro AvroParquet criado pela class AvroParquet e responder as queries
//Os resultados serao guardados em ficheiros de texto separado
//Usado para verificacao de resultados
public class FromParquetToParquetFile{

    public static List<String> top10 = new ArrayList<>();
    public static List<String> sortedTop10 = new ArrayList<>();

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

    //Combiner para responder as queries
    public static class FromParquetQueriesCombiner extends Reducer<Text,Text, Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long total = 0;
            long maior = -1;
            String tconst = "";
            String title = "";

            for (Text value : values) {

                total++; //numero de filmes
                String[] fields = value.toString().split("\t");
                top10.add(key.toString() + "\t" + fields[2] + "\t" + fields[3] + "\t" + fields[0] + "\t" + fields[1]);
                int x = Integer.parseInt(fields[3]);

                if (x >= maior) {
                    tconst = fields[0]; //filme com mais votos nas chaves que juntou (nao sao todas as entradas do ano, uma vez que o resto das entradas (continuacao)
                    title = fields[1];  //podem ter ido para outro combiner)
                    maior = x;
                }
            }

            StringBuilder result = new StringBuilder();
            result.append(total);
            result.append("\t");
            result.append(tconst);
            result.append("\t");
            result.append(title);
            result.append("\t");
            result.append(maior);
            //preciso mandar para o reducer assim a chave para ele juntar uma vez que o combiner nao junta as chaves todas iguais, so as que
            //vem para ele e uma vez que do mapper podem vir varios splits cada combiner trata de um split e o reducer e que junta tudo
            //combiner vai servir para tirar algum trabalho do reducer e assim algumas chaves ja estao juntas
            context.write(key, new Text(result.toString()));
        }
    }

    //Reducer para responder as queries
    public static class FromParquetQueriesReducer extends Reducer<Text, Text, Void, GenericRecord> {
        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Collections.sort(top10, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    String[] aux = o1.split("\t");
                    String[] aux2 = o2.split("\t");

                    Double rating = Double.parseDouble(aux[1]);
                    Double rating2 = Double.parseDouble(aux2[1]);

                    int result = rating.compareTo(rating2);
                    if(result == 0){
                        Integer vote = Integer.parseInt(aux[2]);
                        Integer vote2 = Integer.parseInt(aux2[2]);
                        result = vote.compareTo(vote2);
                    }
                    return -result;
                }
            });
            System.out.println(top10.size());
            schema = getSchema("hdfs:///schema.alinea2");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long total_movies = 0;
            long most_votes = -1;
            String tconst_most_votes = "";
            String title_most_votes = "";
            GenericRecord record = new GenericData.Record(schema);

            for(Text t : values){
                //fields[0] = total ; fields[1] = tconst ; fields[2] = title ; fields[3] = maior
                String[] fields = t.toString().split("\t");
                total_movies += Long.parseLong(fields[0]);

                int field_with_most_votes = Integer.parseInt(fields[3]);
                if (field_with_most_votes >= most_votes) {
                    tconst_most_votes = fields[1]; //filme com mais votos nas chaves que juntou (nao sao todas as entradas do ano, uma vez que o resto das entradas (continuacao)
                    title_most_votes = fields[2];  //podem ter ido para outro combiner)
                    most_votes = field_with_most_votes;
                }
            }

            List<String> result = new ArrayList<>();

            String ano = key.toString();
            int count = 0;
            for(String s : top10){
                //ano rating votes tconst title
                if(count == 10) break;

                String[] aux = s.split("\t");

                if(aux[0].equals(ano) && !aux[1].equals("-1")){
                    result.add(aux[3] + "\t" + aux[4] + "\t" + aux[1] + "\t" + aux[2]);
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

