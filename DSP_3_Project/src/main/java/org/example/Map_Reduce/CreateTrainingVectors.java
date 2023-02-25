package org.example.Map_Reduce;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.AWS_Services.S3Instance;
import org.example.CreateJobs.ClassNames;
import org.example.CreateJobs.InputOutputNames;
import org.example.CreateJobs.MainArgs;
import software.amazon.awssdk.regions.Region;
import java.io.IOException;
import java.util.HashMap;

public class CreateTrainingVectors {
    public static class MapperClassFilterIrrelevantDependenciesOutput extends Mapper<Text, Text, Text, Text> {
        HashMap<String /*dependencyPath*/, Integer /*index*/> dependencyPathIndex = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            int count = 0;
            //load GetRelevantDependencies.txt and init
            S3Instance s3 = new S3Instance(Region.US_EAST_1, MainArgs.getOutputBucket()); //fixme: change to your bucket name
            String allDependencies = s3.downloadFileContentFromS3(
                    InputOutputNames.get(ClassNames.GetRelevantDependencies).output + "/part-r-00000",
                    "txt",
                    "src/main/java/org/example/Map_Reduce/Outputs");

            String[] dependencies = allDependencies.split("\n");
            for (String dependency : dependencies) {
                dependencyPathIndex.put(dependency, count++);
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
        }
    }

    public static class MapperClassHypernym extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text firstWord, Text secondWordAndLabel, Context context) throws IOException, InterruptedException {
            String[] parts = secondWordAndLabel.toString().split("\t");
            String secondWord = parts[0];
            String label = parts[1];
            context.write(new Text(firstWord + " " + secondWord), new Text("table2" + "\t" + label)); //fixed. now the key is the noun pair
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private int NUMBER_OF_RELEVANT_DEPENDENCIES_PATHS = -1;
        private int[] vector = null;

        @Override
        public void reduce(Text noun_pair, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String label = null;
            NUMBER_OF_RELEVANT_DEPENDENCIES_PATHS = -1;
            vector = null;

            for (Text value : values) {
                String[] tableAndValue = value.toString().split("\t");
                if (tableAndValue[0].equals("table2"))
                    label = tableAndValue[1];
                else updateVector(tableAndValue);
            }

            if (label == null || NUMBER_OF_RELEVANT_DEPENDENCIES_PATHS == -1)
                return;
            context.write(noun_pair, new Text(convertVectorToText() + "\t" + label));
        }

        private void updateVector(String[] tableAndValue) {
            assert tableAndValue[0].equals("table1");
            if (NUMBER_OF_RELEVANT_DEPENDENCIES_PATHS == -1) {
                NUMBER_OF_RELEVANT_DEPENDENCIES_PATHS = Integer.parseInt(tableAndValue[3]);
                vector = new int[NUMBER_OF_RELEVANT_DEPENDENCIES_PATHS];
            }

            int index = Integer.parseInt(tableAndValue[1]);
            int count = Integer.parseInt(tableAndValue[2]);
            vector[index] += count;
        }

        private Text convertVectorToText() {
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
