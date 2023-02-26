package org.example.Map_Reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.CreateJobs.MainArgs;
import org.example.DependancyTree.Graph;

import java.io.IOException;
import java.util.HashMap;

public class FilterIrrelevantDependencies {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Graph g = new Graph(value.toString());
                for (String path : g.getFullDependencyPathFromNounToNoun()) {
                    if (path == null) break;
                    String[] splitBySpace = path.split(" ");
                    String firstWord = splitBySpace[0];
                    String lastWord = splitBySpace[splitBySpace.length - 2];
                    String start = splitBySpace[0] + splitBySpace[1];
                    int trimStartIndex = start.length() + 2;
                    String end = splitBySpace[splitBySpace.length - 1] + splitBySpace[splitBySpace.length - 2];
                    int trimEndIndex = path.length() - end.length() - 2;
                    String pathWithoutStringAndEnd = path.substring(trimStartIndex, trimEndIndex);

                    context.write(new Text(pathWithoutStringAndEnd), new Text(firstWord + " " + lastWord + "\t" + g.getCount()));
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("troll mapper " + value.toString());
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text dependencyPath, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            try {
                HashMap<String, Integer> uniquePairsCount = new HashMap<>();
                for (Text pairAndCont : values) {
                    System.out.println("[DEBUG] pairAndCont: " + pairAndCont.toString());
                    String[] parts = pairAndCont.toString().split("\t");
                    String pair = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    uniquePairsCount.put(pair, uniquePairsCount.getOrDefault(pair, 0) + count);
                }

                if (uniquePairsCount.keySet().size() < MainArgs.getDpMin())
                    //irrelevant
                    return;

                //emit <w1>\s<w2>, <dependency path>\t<count>
                for (String pair : uniquePairsCount.keySet()) {
                    context.write(new Text(pair), new Text(dependencyPath.toString() + "\t" + uniquePairsCount.get(pair)));
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("troll reducer " + dependencyPath.toString());
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
