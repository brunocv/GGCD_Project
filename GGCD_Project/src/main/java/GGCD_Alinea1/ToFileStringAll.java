package GGCD_Alinea1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

//Class que da um ficheiro de texto com informacao presente em basics e ratings
//Usada para Debugging
public class ToFileStringAll {

    //Mapper que trata do ficheiro title.basics.tsv.gz
    public static class ToFileAllMapperLeft extends Mapper<LongWritable, Text, Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //passar a primeira linha
            if (key.get() == 0) return;

            String[] fields = value.toString().split("\t");

            //guardar numa string toda a informacao (tratar de nulos)
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
    public static class ToFileAllMapperRight extends Mapper<LongWritable, Text, Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //passar a primeira linha
            if (key.get() == 0) return;

            String[] fields = value.toString().split("\t");

            //primeira sub-string: rating || segunda sub-string: votes
            StringBuilder values = new StringBuilder();

            //meter um R no inicio para depois no reducer saber se esta e a String (Text) de ratings ou nao, sendo que
            //queremos que esta String venha depois da String (Text) resultante do ToFileAllMapperLeft
            values.append("R");
            values.append("\t");
            values.append(fields[1]);
            values.append("\t");
            values.append(fields[2]);

            context.write(new Text(fields[0]), new Text(values.toString()));
        }
    }

    //Juntar a informacao dos 2 Mappers para um ficheiro de texto
    public static class JoinReducer extends Reducer<Text,Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            boolean has_basics = false;
            boolean has_ratings = false;

            //criadas 2 strings para manter a ordem que queremos
            String basics = "";
            String ratings = "";

            for(Text value : values){
                String[] aux = value.toString().split("\t");

                //se nao tiver R no inicio entao estamos perante o value de basics
                if(!aux[0].equals("R")){
                    basics = value.toString();
                    has_basics = true;
                }
                else if(aux[0].equals("R")){
                    ratings = aux[1] + "\t" + aux[2];
                    has_ratings = true;
                }
            }

            //se tiver as duas entradas entao guardo, se so tiver basics crio ratings a nulo (porque se tem ja sei que e movie)
            //se so tiver ratings nao posso criar porque nao sei se era movie
            if(has_basics && has_ratings){
                String result = basics + ratings;
                context.write(key,new Text(result));
            }
            else if(has_basics && has_ratings == false){
                ratings = "null\tnull";
                String result = basics + ratings;
                context.write(key,new Text(result));
            }
            else if(has_ratings && has_basics == false) return;

        }
    }

    //Main
    public static void main(String[] args) throws Exception{

        long startTime = System.nanoTime();

        Job job1 = Job.getInstance(new Configuration(), "ToFileStringAll");
        job1.setJarByClass(ToFileStringAll.class);

        //input
        job1.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job1,new Path("hdfs:///title.basics.tsv.gz"),
                TextInputFormat.class, ToFileAllMapperLeft.class);

        MultipleInputs.addInputPath(job1,new Path("hdfs:///title.ratings.tsv.gz"),
                TextInputFormat.class, ToFileAllMapperRight.class);

        job1.setReducerClass(JoinReducer.class);

        //output
        //Para guardar em ficheiro (texto)
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1,new Path("hdfs:///resultado_text_all"));
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.waitForCompletion(true);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000; //miliseconds
        System.out.println("\n\nTIME: " + duration +"\n");
    }
}

