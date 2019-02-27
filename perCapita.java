// Griffin Aswegan & Steven Bradley
// gaswegan          stbradle
// CSC 369
// March 1, 2019

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.alexholmes.json.mapreduce.MultiLineJsonInputFormat;
import org.json.JSONObject;

import org.apache.hadoop.fs.Path;

import java.util.Iterator;
import java.io.IOException;

public class perCapita extends Configured implements Tool {

   public static class PopMapper
    extends Mapper<LongWritable, Text, Text, Text> {
   
      public void map(LongWritable key, Text value, Context context)
       throws IOException, InterruptedException {
     
         String county = "";
         String population = "";
         String output;
         try {
            JSONObject json = new JSONObject(value.toString());
            Iterator<String> iter = json.keys();
            while (iter.hasNext()) {
               String jsonKey = iter.next();
               if ("county".equals(jsonKey))
                  county = json.getString(jsonKey);
               if ("population".equals(jsonKey))
                  population = json.getString(jsonKey);
            }
            county = county.replaceAll("'", "").replaceAll(" County", "")
               .replaceAll(" ", "").replaceAll("\t", "");
            output = "P>" + population;
            context.write(new Text(county), new Text(output));

         } catch (Exception e) { System.out.println(e); }
      
      }
   }

    public static class InvoiceMapper
     extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            String county = line[2];
            String sales = line[line.length - 1];
            String output = "I>" + sales;
            county = county.replaceAll("'", "").replaceAll(" ", "")
               .replaceAll("\t", "");;
            context.write(new Text(county), new Text(output));
         }
   }

   public static class JoinReducer
    extends Reducer<Text, Text, Text, Text> {

      public void reduce(Text key, Iterable<Text> values, Context context)
       throws IOException, InterruptedException {
         double salesSum = 0;
         int population = 0;
         String[] line;

         for (Text val : values) {
            line = val.toString().split(">");
            if ("I".equals(line[0]))
               salesSum += Double.parseDouble(line[1]);
            if ("P".equals(line[0]))
               population = Integer.parseInt(line[1].replaceAll(",", "").replaceAll(" ", ""));
         }

         double perCapita = salesSum / population;
         context.write(key, new Text("" + salesSum + "\t" + perCapita));
       }
    }

   @Override 
   public int run(String[] args) throws Exception {
      Configuration conf = super.getConf();
      Job job = Job.getInstance(conf, "Per Capita");
      job.setJarByClass(perCapita.class);

      MultipleInputs.addInputPath(job, new Path("/data/", "iowa.csv"),
                                  TextInputFormat.class, InvoiceMapper.class);
      MultipleInputs.addInputPath(job, new Path("/data/", "counties-alt.json"),
                                  MultiLineJsonInputFormat.class, PopMapper.class);
      FileOutputFormat.setOutputPath(job, new Path("./Lab06/Prog1", "output"));
      MultiLineJsonInputFormat.setInputJsonMember(job,"id");

      job.setReducerClass(JoinReducer.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      return job.waitForCompletion(true) ? 0 : 1;
   }

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      int res = ToolRunner.run(conf, new perCapita(), args);
      System.exit(res);
   }
}
