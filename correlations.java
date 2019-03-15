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

public class correlations {

   public static class CMapper
    extends Mapper<LongWritable, Text, Text, Text> {

      public void map(LongWritable key, Text value, Context context)
       throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			String temp = "" + line[5].replace("\"", "") + ">" + line[7].replace("\"", "");
			Text out = new Text(temp);

			if ("gender".equals(line[0]))
				return;

			context.write(new Text(line[0]), out);
			context.write(new Text(line[1]), out);
			context.write(new Text(line[3]), out);
      }
   }


   public static class CCombiner
    extends Reducer<Text, Text, Text, Text> {
      public void reduce (Text key, Iterable<Text> values, Context context)
       throws IOException, InterruptedException {
         String[] line;
         int count = 0, i;
         double mSum = 0, wSum = 0;
         LinkedList<Integer> mScores = new LinkedList();
         LinkedList<Integer> wScores = new LinkedList();
         try {
            for (Text val : values) {
               line = val.toString().split(">");
               mScores.add(Integer.parseInt(line[0]));
               wScores.add(Integer.parseInt(line[1]));
               count++;
            }
         } catch (Exception e) {
            return;
         }
         for (i = 0; i < mScores.size(); i++) {
            mSum += mScores.get(i);
            wSum += wScores.get(i);
            context.write(key, new Text("O>" + mScores.get(i) + ">" + wScores.get(i))); 
         }
         mSum /= count;
         wSum /= count;
         context.write(key, new Text("S>" + mSum + ">" + wSum));
      }
   }

   public static class CReducer
    extends Reducer<Text, Text, Text, Text> {

      public void reduce(Text key, Iterable<Text> values, Context context)
       throws IOException, InterruptedException {
         String[] line;
         LinkedList<Integer> mScores = new LinkedList();
         LinkedList<Integer> wScores = new LinkedList();
         int topSum = 0, bottomLSum = 0, bottomRSum = 0, i;
         double muMath = 1, muWrite = 1;
         double coeff = 0;

         for (Text val : values) {
            line = val.toString().split(">");
            if ("S".equals(line[0])) {
               muMath = Double.parseDouble(line[1]);
               muWrite = Double.parseDouble(line[2]);
            }
            else {
               mScores.add(Integer.parseInt(line[1]));
               wScores.add(Integer.parseInt(line[2]));
            }
         }

         for (i = 0; i < mScores.size(); i++) {
            topSum += (mScores.get(i) - muMath) * (wScores.get(i) - muWrite);
            bottomLSum += (mScores.get(i) - muMath) * (mScores.get(i) - muMath);
            bottomRSum += (wScores.get(i) - muWrite) * (mScores.get(i) - muWrite);
         }

         coeff = topSum / (Math.sqrt(bottomLSum) * Math.sqrt(bottomRSum));
         context.write(key, new Text("" + coeff));
      }
   }

   public static void main(String[] args) throws Exception {
      Job job1 = Job.getInstance();
      job1.setJarByClass(correlations.class);

      TextInputFormat.addInputPath(job1, new Path("/data/", "StudentsPerformance.csv"));
      FileOutputFormat.setOutputPath(job1, new Path("./Lab06/Prog5", "output"));

      job1.setMapperClass(CMapper.class);
      job1.setCombinerClass(CCombiner.class);
      job1.setReducerClass(CReducer.class);

      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);

      job1.setJobName("Correlations");

      System.exit(job1.waitForCompletion(true) ? 0 : 1);
   }
}
