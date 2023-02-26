package org.example.Map_Reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GetRelevantDependencies {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        public void map(LongWritable key, Text value /*<pair>\t<dependency>\t<count>*/, Context context) throws IOException, InterruptedException {
            String dependencyPath = value.toString().split("\t")[1];
            context.write(new Text(dependencyPath), new Text());
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text dependencyPath, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(dependencyPath, new Text());
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
