package org.example.Map_Reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CreateTrainingVectors {
    public static class MapperClassFilterIrrelevantDependenciesOutput extends Mapper<Text, Text, Text, Text> {
        BufferedReader reader;
        HashMap<String /*dependencyPath*/, Integer /*index*/> dependencyPathIndex = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            int count = 0;
            //load GetRelevantDependencies.txt and init
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            Path filePath = fileSplit.getPath();
            FSDataInputStream inputStream = fs.open(filePath);
            reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                dependencyPathIndex.put(line, count++);
            }

        }

        @Override
        public void map(Text noun_pair, Text dependencyPathAndCount, Context context) throws IOException, InterruptedException {
            String[] parts = dependencyPathAndCount.toString().split("\t");
            String dependencyPath = parts[0];
            String count = parts[1];
            context.write(noun_pair, new Text("table1" + "\t" + dependencyPathIndex.get(dependencyPath) + "\t" + count + "\t" + dependencyPathIndex.size()));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            reader.close();
        }
    }

    public static class MapperClassHypernym extends Mapper<Text, Text, Text, Text> {


        @Override
        public void map(Text firstWord, Text SecondWordAndLabel, Context context) throws IOException, InterruptedException {
            context.write(firstWord, new Text("table2" + "\t" + SecondWordAndLabel));//fixme - check if the key should include both words or only the first one.
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text noun_pair, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String label = null;
            int NUMBER_OF_RELEVANT_DEPENDENCIES_PATHS = 0;
            for (Text value : values) {
                String[] tableAndValue = value.toString().split("\t");
                if (tableAndValue[0].equals("table2")) {
                    label = tableAndValue[1];
                } else
                    NUMBER_OF_RELEVANT_DEPENDENCIES_PATHS = Integer.parseInt(tableAndValue[3]);

            }
            if (label == null) return;
            int[] vector = new int[NUMBER_OF_RELEVANT_DEPENDENCIES_PATHS];
            for (Text value : values) {
                String[] tableAndValue = value.toString().split("\t");
                if (tableAndValue[0].equals("table1")) {
                    int index = Integer.parseInt(tableAndValue[1]);
                    int count = Integer.parseInt(tableAndValue[2]);
                    vector[index] += count;
                }
            }
            context.write(noun_pair, new Text(convertVectorToText(vector) + "\t" + label));
        }

        private Text convertVectorToText(int[] vector) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < vector.length; i++) {
                sb.append(vector[i]);
                if (i != vector.length - 1) sb.append(",");
            }
            return new Text(sb.toString());
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
