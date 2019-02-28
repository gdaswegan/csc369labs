// Griffin Aswegan & Steven Bradley
// gaswegan          stbradle
// CSC 369
// March 1, 2019

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;

import java.util.LinkedList;
import java.lang.Math;
import java.io.IOException;

public class correlation {

    public static class CorrelationMapper
     extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {
            String[] line = value.toString().split(",");

            if (line[line.length - 3].contains("Rum") || 
                line[line.length - 3].contains("rum") ||
                line[line.length - 4].contains("Rum") ||
                line[line.length - 4].contains("rum"))
               context.write(new Text(line[2]), new Text("rum"));

            if (line[line.length - 3].contains("Vodka") || 
                line[line.length - 3].contains("vodka") ||
                line[line.length - 4].contains("Vodka") ||
                line[line.length - 4].contains("vodka"))
               context.write(new Text(line[2]), new Text("vodka")); 
        }
   }

   public static class CorrelationMapper2
    extends Mapper<LongWritable, Text, Text, Text> {
       public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
          String[] line = value.toString().split("\t");
          context.write(new Text(line[0]), new Text(line[1] + ">" + line[2]));
       }
   }

   public static class CorrelationReducer
    extends Reducer<Text, Text, Text, Text> {

       public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
         int rumCount = 0, vodkaCount = 0;
         String resp;

         for (Text val : values) {
            resp = val.toString();
            if ("rum".equals(resp))
               rumCount++;
            if ("vodka".equals(resp))
               vodkaCount++;
         }
         
         context.write(key, new Text("" + rumCount + "\t" + vodkaCount));

       }
   }

   public static class CorrelationReducer2
    extends Reducer<Text, Text, Text, Text> {
       int count;
       LinkedList<Integer> rums;
       LinkedList<Integer> vodkas;

       public void setup(Context context) {
         count = 0;

         rums = new LinkedList();
         vodkas = new LinkedList();
       }

       public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            String[] line = null;
            int rum, vodka;
      
            for (Text val : values)
               line = val.toString().split(">");
            
            if (line == null)
               throw new IOException();

            rum = Integer.parseInt(line[0]);
            vodka = Integer.parseInt(line[1]);
            
            count += 1;
            
            rums.add(rum);
            vodkas.add(vodka);
       }

       public void cleanup(Context context) 
        throws IOException, InterruptedException {
         double muRum = 0, muVodka = 0, rumVal, vodkaVal;
         double topSum = 0, bottomSumL = 0, bottomSumR = 0, bottomSum;
         int i;

         for (i = 0; i < count; i++) {
            muRum += rums.get(i);
            muVodka += vodkas.get(i);
         }

         muRum /= count;
         muVodka /= count;

         for (i = 0; i < count; i++) {
             rumVal = rums.get(i) - muRum;
             vodkaVal = vodkas.get(i) - muVodka;
             topSum += rumVal * vodkaVal;
             bottomSumL += rumVal * rumVal;
             bottomSumR += vodkaVal * vodkaVal;
         }

         bottomSum = Math.sqrt(bottomSumL) * Math.sqrt(bottomSumR);
         context.write(new Text("pearson"), new Text("" + (topSum / bottomSum)));
       }
   }

   public static void main(String[] args) throws Exception {
      Job job1 = Job.getInstance();
      job1.setJarByClass(correlation.class);

      TextInputFormat.addInputPath(job1, new Path("/data/", "iowa.csv"));
      FileOutputFormat.setOutputPath(job1, new Path("./Lab06/Prog2", "temp"));
     
      job1.setMapperClass(CorrelationMapper.class);
      job1.setReducerClass(CorrelationReducer.class);
      
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);

      job1.setJobName("Correlation Part 1");
      job1.waitForCompletion(true);

      Job job2 = Job.getInstance();
      job2.setJarByClass(correlation.class);

      TextInputFormat.addInputPath(job2, new Path("./Lab06/Prog2/temp/", 
       "part-r-00000"));
      FileOutputFormat.setOutputPath(job2, new Path("./Lab06/Prog2/", "output"));

      job2.setMapperClass(CorrelationMapper2.class);
      job2.setReducerClass(CorrelationReducer2.class);

      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);

      job2.setJobName("Correlation Part 2");
      System.exit(job2.waitForCompletion(true) ? 0 : 1);
   }
}
