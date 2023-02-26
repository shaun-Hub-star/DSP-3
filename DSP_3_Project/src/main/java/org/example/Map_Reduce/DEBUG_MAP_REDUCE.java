package org.example.Map_Reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.example.CreateJobs.MainArgs;
import org.example.DependancyTree.Graph;

import java.io.IOException;
import java.util.HashMap;

public class DEBUG_MAP_REDUCE {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(""), new Text("1"));
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text ignore : values) {
                sum++;
            }
            context.write(new Text("sum:"), new Text("" + sum));

        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
