// Griffin Aswegan & Steven Bradley
// gaswegan          stbradle
// CSC 369
// March 1, 2019

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.*;
import java.lang.Math;
import java.io.IOException;

public class Dice {
   public static boolean isStopword (String w) {
      //TODO: ADD LOGIC FOR CHECKING STOPWORDS IF NEEDED
      return false;
   }
   public static class WordCountMapper
         extends Mapper<LongWritable, Text, Text, Text>
   {
      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         String[] words = value.toString().split("\\W");
         String inFile =
               ((FileSplit)context.getInputSplit()).getPath().toString();
         for (String w: words){
            if(!isStopword(w))
               context.write(new Text(inFile + ">>" + w), new Text());
         }
      }
   }

   public static class WordCountReducer
         extends Reducer<Text, Text, Text, Text>
   {
      @Override
      public void reduce (Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
      {
         String[] line = key.toString().split(">>");
         String fileName = line[0];
         String word = line.length > 1 ? line[1] : "";
         List<Text> words = new ArrayList<>();
         values.forEach(words::add);

         context.write(
            new Text(word),
            new Text(fileName + "," + words.size()));
      }
   }

   public static class TopKMapper
      extends Mapper<LongWritable, Text, Text, Text>
   {
      HashMap<String, PriorityQueue<QueueNode>> queueMap;
      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
         super.setup(context);
         queueMap = new HashMap<>();
      }

      @Override
      public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException
      {
         String[] line = value.toString().split("\\s");
         String[] vals = line[1].split(",");
         String word = line[0];
         String fileName = vals[0];
         int count = Integer.valueOf(vals[1]);
         PriorityQueue<QueueNode> queue;

         if(queueMap.containsKey(fileName)) {
            queue = queueMap.get(fileName);
         } else {
            queue = new PriorityQueue<>(100);
         }

         if (queue.size() < 100)
            queue.add(new QueueNode(word, count));
         else {
            QueueNode current =
                  new QueueNode(word, count);
            QueueNode min = queue.peek();
            if (min != null && current.getWeight() > min.getWeight()) {
               queue.poll();
               queue.add(current);
            }
         }
         queueMap.put(fileName, queue);
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
         super.cleanup(context);
         for(String key: queueMap.keySet()) {
            PriorityQueue<QueueNode> queue = queueMap.get(key);
            while (!queue.isEmpty()) {
               QueueNode cur = queue.poll();
               Text value = new Text(cur.getWord() + "<<" + cur.getWeight());
               context.write(new Text(key), value);
            }
         }
      }

   }

   public static class TopKReducer
      extends Reducer<Text, Text, Text, Text>{
      PriorityQueue<QueueNode> queue;

      @Override
      protected void reduce(Text key, Iterable<Text> values, Context context)
         throws IOException, InterruptedException
      {
         queue = new PriorityQueue<>();
         for(Text value: values){
            String[] line = value.toString().split("<<");
            if (queue.size() < 100)
               queue.add(new QueueNode(line[0], Integer.valueOf(line[1])));
            else {
               QueueNode current =
                     new QueueNode(line[0], Integer.valueOf(line[1]));
               QueueNode min = queue.peek();
               if(min != null && current.getWeight() > min.getWeight()){
                  queue.poll();
                  queue.add(current);
               }
            }
         }

         while(!queue.isEmpty())
            context.write(new Text(queue.poll().getWord()), new Text(key));
      }

      @Override
      protected void cleanup(Context context)
         throws IOException, InterruptedException
      {
         super.cleanup(context);
      }
   }

   public static void main(String[] args) throws Exception {
      Job wordCountJob = Job.getInstance();
      wordCountJob.setJarByClass(Dice.class);

      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "11-0.txt"));
      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "1342-0.txt"));

      FileOutputFormat.setOutputPath(wordCountJob, new Path("./Lab06/Prog3", "counts"));

      wordCountJob.setMapperClass(WordCountMapper.class);
      wordCountJob.setReducerClass(WordCountReducer.class);

      wordCountJob.setOutputKeyClass(Text.class);
      wordCountJob.setOutputValueClass(Text.class);

      wordCountJob.setJobName("Dice");
      wordCountJob.waitForCompletion(true);

      Job topKJob = Job.getInstance();
      topKJob.setJarByClass(Dice.class);
      FileInputFormat.addInputPath(topKJob, new Path("./Lab06/Prog3/counts", "part-r-00000"));
      FileOutputFormat.setOutputPath(topKJob, new Path("./Lab06/Prog3", "topK"));
      topKJob.setMapperClass(TopKMapper.class);
      topKJob.setReducerClass(TopKReducer.class);
      topKJob.setOutputKeyClass(Text.class);
      topKJob.setOutputValueClass(Text.class);

      topKJob.setJobName("Dice");
      System.exit(topKJob.waitForCompletion(true) ? 0 : 1);
   }
}