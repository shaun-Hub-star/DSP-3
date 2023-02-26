package org.example.CreateJobs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.example.Map_Reduce.CreateTrainingVectors;
import org.example.Map_Reduce.DEBUG_MAP_REDUCE;
import org.example.Map_Reduce.FilterIrrelevantDependencies;
import org.example.Map_Reduce.GetRelevantDependencies;
import org.example.TrainingModel.CalculateClassifier;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MainJobs {
   // private static final Logger LOG = Logger.getLogger(FilterIrrelevantDependencies.MapperClass.class);

    @SuppressWarnings("all")
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        MainArgs.init(args);
        InputOutputNames.init();

        Job debugJob = troll();
        if(!debugJob.waitForCompletion(true))
            System.exit(1);

        //LOG.setLevel(Level.DEBUG);
        /*
        Job filterIrrelevantDependenciesJob = filterIrrelevantDependenciesJob(MainArgs.getDpMin());
        if(!filterIrrelevantDependenciesJob.waitForCompletion(true))
            System.exit(1);

        Job getRelevantDependencies = getRelevantDependenciesJob();
        if(!getRelevantDependencies.waitForCompletion(true))
            System.exit(1);

        Job createTrainingVecotorsJob = createTrainingVectorsJob();
        if(!createTrainingVecotorsJob.waitForCompletion(true))
            System.exit(1);

        String trainingVectorsPath = MainArgs.getOutputPath().substring("s3://".length());
        CalculateClassifier calculateClassifier = new CalculateClassifier(MainArgs.getOutputBucket(), trainingVectorsPath);
        calculateClassifier.trainModelNew();
        calculateClassifier.modelStats();

        /*JobBuilder.mergeOutput(InputOutputNames.get(ClassNames.WordCount).output,
            InputOutputNames.get(ClassNames.WordCount).output + "/" + InputOutputNames.outputName);*/


    }
    private static Job filterIrrelevantDependenciesJob(int DP_MIN) throws IOException{
        System.out.println("Output path: " + InputOutputNames.get(ClassNames.FilterIrrelevantDependencies).output);
        return JobBuilder.builder()
                .jarByClass(FilterIrrelevantDependencies.class)
                .jobName("filter irrelevant dependencies")
                .mapperClass(FilterIrrelevantDependencies.MapperClass.class)
                .partitionerClass(FilterIrrelevantDependencies.PartitionerClass.class)
                //.combinerClass(FilterIrrelevantDependencies.CombinerClass.class)
                .reducerClass(FilterIrrelevantDependencies.ReducerClass.class)
                .inputPath(InputOutputNames.get(ClassNames.FilterIrrelevantDependencies).inputs[0])
                .outputPath(InputOutputNames.get(ClassNames.FilterIrrelevantDependencies).output)
                //.numberOfReducers(1)
                .setVariable(DP_MIN)
                //.inputFormatClass(SequenceFileInputFormat.class)//FIXME: for debugging
                .build();
    }

    private static Job getRelevantDependenciesJob() throws IOException{
        return JobBuilder.builder()
                .jarByClass(GetRelevantDependencies.class)
                .jobName("get relevant dependencies")
                .mapperClass(GetRelevantDependencies.MapperClass.class)
                .partitionerClass(GetRelevantDependencies.PartitionerClass.class)
                //.combinerClass(GetRelevantDependencies.CombinerClass.class)
                .reducerClass(GetRelevantDependencies.ReducerClass.class)
                .inputPath(InputOutputNames.get(ClassNames.GetRelevantDependencies).inputs[0])
                .outputPath(InputOutputNames.get(ClassNames.GetRelevantDependencies).output)
                .numberOfReducers(1)
                //.inputFormatClass(SequenceFileInputFormat.class)//FIXME: for debugging
                .build();
    }

    private static Job createTrainingVectorsJob() throws IOException, URISyntaxException {
        String[] inputs = InputOutputNames.get(ClassNames.CreateTrainingVectors).inputs;
        return JobBuilder.builder()
                .jarByClass(CreateTrainingVectors.class)
                .jobName("create training vectors")
                .partitionerClass(CreateTrainingVectors.PartitionerClass.class)
                .reducerClass(CreateTrainingVectors.ReducerClass.class)
                .addInputPath(inputs[0], CreateTrainingVectors.MapperClassFilterIrrelevantDependenciesOutput.class)
                .addInputPath(inputs[1], CreateTrainingVectors.MapperClassHypernym.class)
                .outputPath(InputOutputNames.get(ClassNames.CreateTrainingVectors).output)
                .cacheFile(new URI(InputOutputNames.get(ClassNames.GetRelevantDependencies).output))
                .numberOfReducers(1)
                .build();
    }

    private static Job troll() throws IOException {
        return JobBuilder.builder()
                .jarByClass(DEBUG_MAP_REDUCE.class)
                .jobName("DEBUG_MAP_REDUCE")
                .mapperClass(DEBUG_MAP_REDUCE.MapperClass.class)
                .partitionerClass(DEBUG_MAP_REDUCE.PartitionerClass.class)
                //.combinerClass(GetRelevantDependencies.CombinerClass.class)
                .reducerClass(DEBUG_MAP_REDUCE.ReducerClass.class)
                .inputPath(InputOutputNames.get(ClassNames.DEBUG_MAP_REDUCE).inputs[0])
                .outputPath(InputOutputNames.get(ClassNames.DEBUG_MAP_REDUCE).output)
                .numberOfReducers(1)
                //.inputFormatClass(SequenceFileInputFormat.class)//FIXME: for debugging
                .build();
    }


}
