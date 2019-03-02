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
import java.util.stream.Collectors;

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
   }
   public static class DiceMapper
         extends Mapper<LongWritable, Text, Text, Text>
   {
      HashMap<String, LinkedList<String>> listMap;
      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
         super.setup(context);
         listMap = new HashMap<>();
      }

      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
      {
         String[] line = value.toString().split("\\s");
         String word = line[0];
         String fileName = line[1];
         LinkedList<String> list;

         if(listMap.containsKey(fileName)) {
            list = listMap.get(fileName);
         } else {
            list = new LinkedList<>();
         }
         list.add(word);

         listMap.put(fileName, list);
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
         super.cleanup(context);
         Deque<Map.Entry<String, LinkedList<String>>> queue =
               new ArrayDeque<>(listMap.entrySet());

         while(!queue.isEmpty()){
            Map.Entry<String, LinkedList<String>> e = queue.pop();

            String[] pathToFile = e.getKey().split("/");
            String fileName = pathToFile[pathToFile.length-1];

            LinkedList<String> words = e.getValue();

            for(Map.Entry<String, LinkedList<String>> entry: queue){
               String [] pathToFile1 = entry.getKey().split("/");
               String fileName1 = pathToFile1[pathToFile1.length-1];

               LinkedList<String> words1 =  entry.getValue();

               int numCommonWords = words.stream()
                     .filter(words1::contains)
                     .collect(Collectors.toList())
                     .size();

               context.write(
                     new Text(fileName + ">>" + fileName1),
                     new Text(Integer.toString(numCommonWords)));
            }
         }
      }

   }

   public static class DiceReducer
         extends Reducer<Text, Text, Text, Text>{
      PriorityQueue<QueueNode> queue;

      @Override
      protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
      {
         String[] documents = key.toString().split(">>");
         String doc1 = documents[0];
         String doc2 = documents[1];
         int commonWords = 0;
         for(Text value : values){
            commonWords += Integer.valueOf(value.toString());
         }
         double diceCoef = 2 * commonWords /(200d);

         context.write(
               new Text (doc1 + " " + doc2),
               new Text(Double.toString(diceCoef)));
      }
   }

   public static void main(String[] args) throws Exception {
      Job wordCountJob = Job.getInstance();
      wordCountJob.setJarByClass(Dice.class);

      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "11-0.txt"));
      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "1342-0.txt"));
      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "1952-0.txt"));
      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "219-0.txt"));
      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "2701-0.txt"));
      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "76-0.txt"));
      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "84-0.txt"));
      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "98-0.txt"));
      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "pg1080.txt"));
      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "pg1661.txt"));
      FileInputFormat.addInputPath(wordCountJob, new Path("/data/Guttenberg", "pg844.txt"));

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
      topKJob.waitForCompletion(true);

      Job diceJob = Job.getInstance();
      diceJob.setJarByClass(Dice.class);
      FileInputFormat.addInputPath(diceJob, new Path("./Lab06/Prog3/topK", "part-r-00000"));
      FileOutputFormat.setOutputPath(diceJob, new Path("./Lab06/Prog3", "dice"));
      diceJob.setMapperClass(DiceMapper.class);
      diceJob.setReducerClass(DiceReducer.class);
      diceJob.setOutputKeyClass(Text.class);
      diceJob.setOutputValueClass(Text.class);
      diceJob.setJobName("Dice");
      System.exit(diceJob.waitForCompletion(true) ? 0 : 1);
   }
}