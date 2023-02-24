package org.example.Map_Reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.DependancyTree.Graph;

import java.io.IOException;
import java.util.HashMap;

public class FilterIrrelevantDependencies {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Graph g = new Graph(value.toString());
            for(String path : g.getFullDependencyPathFromNounToNoun()){
                String start = path.split(" ")[0];
                String end = path.split(" ")[path.split(" ").length - 1];
                String pathWithoutStringAndEnd = path.substring(start.length() + 1, path.length() - end.length() - 1);
                context.write(new Text(pathWithoutStringAndEnd), new Text(start + " " + end + "\t" + g.getCount()));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text dependencyPath, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> uniquePairsCount = new HashMap<>();
            for(Text pairAndCont : values){
                String[] parts = pairAndCont.toString().split("\t");
                String pair = parts[0];
                int count = Integer.parseInt(parts[1]);
                uniquePairsCount.put(pair, uniquePairsCount.getOrDefault(pair, 0) + count);

            }

            if(uniquePairsCount.keySet().size() < 5)
                //irrelevant
                return;

            //emit <w1>\s<w2>, <dependency path>\t<count>
            for(String pair : uniquePairsCount.keySet()){
                context.write(new Text(pair), new Text(dependencyPath.toString() + "\t" + uniquePairsCount.get(pair)));
            }

        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
