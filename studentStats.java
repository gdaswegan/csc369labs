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

public class studentStats {

   public static class StatsMapper
    extends Mapper<LongWritable, Text, Text, Text> {

      public void map(LongWritable key, Text value, Context context)
       throws IOException, InterruptedException {
         String[] line = value.toString().split(",");
         String temp = "" + line[5].replace("\"", "") + ">" +
          line[6].replace("\"", "") + ">" + line[7].replace("\"", "");
         Text out = new Text(temp);
         
         if ("gender".equals(line[0]))
            return;
         
         context.write(new Text(line[0]), out);
         context.write(new Text(line[1]), out);
         context.write(new Text(line[3]), out);
      }
   }


   public static class StatsReducer
    extends Reducer<Text, Text, Text, Text> {

      public void reduce(Text key, Iterable<Text> values, Context context)
       throws IOException, InterruptedException {
         String[] line = null;
         double mathSum = 0, readSum = 0, writeSum = 0;
         double mathMean, readMean, writeMean,
                mathSTD, readSTD, writeSTD;
         double temp;
         LinkedList<Integer> math = new LinkedList(),
                             read = new LinkedList(),
                             write = new LinkedList();
         int total = 0;
         
         for (Text val : values) {
            line = val.toString().split(">");
            try {
               math.add(Integer.parseInt(line[0]));
            } catch(Exception e) {
               return;
            }

            read.add(Integer.parseInt(line[1]));
            write.add(Integer.parseInt(line[2]));
            total++;
         }
         
         for (total = 0; total < math.size(); total++) {
            mathSum += math.get(total);
            readSum += read.get(total);;
            writeSum += write.get(total);;
         }
         
         mathMean = mathSum / total;
         readMean = readSum / total;
         writeMean = writeSum / total;
         
         mathSum = readSum = writeSum = 0;
         for (total = 0; total < math.size(); total++) {
            temp = math.get(total) - mathMean;
            mathSum += (temp * temp);
            temp = read.get(total) - readMean;
            readSum += (temp * temp);
            temp = write.get(total) - writeMean;
            writeSum += (temp * temp);
         }
         
         mathSTD = Math.sqrt(mathSum / total);
         readSTD = Math.sqrt(readSum / total);
         writeSTD = Math.sqrt(writeSum / total);
         
         context.write(key, new Text("" + mathMean + "\t" + mathSTD + "\t" +
          readMean + "\t" + readSTD + "\t" + writeMean + "\t" + writeSTD));
       }
   }

   public static void main(String[] args) throws Exception {
      Job job1 = Job.getInstance();
      job1.setJarByClass(studentStats.class);

      TextInputFormat.addInputPath(job1, new Path("/data/", "StudentsPerformance.csv"));
      FileOutputFormat.setOutputPath(job1, new Path("./Lab06/Prog4", "output"));

      job1.setMapperClass(StatsMapper.class);
      job1.setReducerClass(StatsReducer.class);

      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);

      job1.setJobName("Student Stats");

      System.exit(job1.waitForCompletion(true) ? 0 : 1);
   }
}
