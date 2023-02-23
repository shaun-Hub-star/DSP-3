package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class  WordCount {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        HashSet<String> stopWords;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            stopWords = new HashSet<>();
            //System.out.println("~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~");
            //System.out.println(context.getCacheFiles()[0]);
            //System.out.println("~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~");
            try (BufferedReader reader = new BufferedReader(new FileReader(String.valueOf(context.getCacheFiles()[0])))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.toLowerCase());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            PartedText word = new PartedText(key, value);
            if(word.getNumOfWords() != 3 || word.hasStopWord(stopWords) || word.hasIllegalCharacter())
                return;

            //emit <3gram>, <part>
            context.write(word.getText(), new Text("" + word.getPart()));
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum0 = 0;
            long sum1 = 0;
            for (Text value : values) {
                String[] sums = value.toString().split("\\s");
                sum0 += Integer.parseInt(sums[0]);
                sum1 += Integer.parseInt(sums[1]);
            }
            context.write(key, new Text(sum0 + "\t" + sum1));
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum0 = 0;
            long sum1 = 0;
            for (Text value : values) {
                if(value.toString().equals("0"))
                    sum0++;
                else
                    sum1++;
            }
            context.write(key, new Text(sum0 + "\t" + sum1));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}