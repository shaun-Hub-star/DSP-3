package org.example.CreateJobs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.example.Map_Reduce.CreateTrainingVectors;
import org.example.Map_Reduce.FilterIrrelevantDependencies;
import org.example.Map_Reduce.GetRelevantDependencies;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MainJobs {


    @SuppressWarnings("all")
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        InputOutputNames.init(new MainArgs(args));
        Job filterIrrelevantDependenciesJob = filterIrrelevantDependenciesJob();
        if(!filterIrrelevantDependenciesJob.waitForCompletion(true))
            System.exit(1);

        Job getRelevantDependencies = getRelevantDependenciesJob();
        if(!getRelevantDependencies.waitForCompletion(true))
            System.exit(1);

        Job createTrainingVecotorsJob = createTrainingVectorsJob();
        if(!createTrainingVecotorsJob.waitForCompletion(true))
            System.exit(1);

        return;

        /*JobBuilder.mergeOutput(InputOutputNames.get(ClassNames.WordCount).output,
            InputOutputNames.get(ClassNames.WordCount).output + "/" + InputOutputNames.outputName);*/

    }

    private static Job filterIrrelevantDependenciesJob() throws IOException{
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
                .inputFormatClass(SequenceFileInputFormat.class)//FIXME: for debugging
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
                .inputFormatClass(SequenceFileInputFormat.class)//FIXME: for debugging
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
                //.numberOfReducers(1)
                .build();
    }


}
