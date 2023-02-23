package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MainJobs {
    private final static double N = 11076496651.0;


    //args[] = {inputInS3, outputInS3, eng-stop-words}
    @SuppressWarnings("all")
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        InputOutputNames.init(args);
        Job wordCountJob = wordCountJob();
        if (!wordCountJob.waitForCompletion(true)) {
            System.exit(1);
        }
        //part
        /*JobBuilder.mergeOutput(InputOutputNames.get(ClassNames.WordCount).output,
                InputOutputNames.get(ClassNames.WordCount).output + "/" + InputOutputNames.outputName);
*/
        Job Nr = NrJob();
        if (!Nr.waitForCompletion(true)) {
            System.exit(1);
        }
  /*      JobBuilder.mergeOutput(InputOutputNames.get(ClassNames.Nr).output,
                InputOutputNames.get(ClassNames.Nr).output + "/" + InputOutputNames.outputName);

*/        Job Tr = TrJob();
        if (!Tr.waitForCompletion(true)) {
            System.exit(1);
        }
  /*      JobBuilder.mergeOutput(InputOutputNames.get(ClassNames.Tr).output,
                InputOutputNames.get(ClassNames.Tr).output + "/" + InputOutputNames.outputName);
*/
        Job jobJoinWordCountNr = joinWordCountNr();
        if (!jobJoinWordCountNr.waitForCompletion(true)) {
            System.exit(1);
        }
  /*      JobBuilder.mergeOutput(InputOutputNames.get(ClassNames.WordCountJoinNr).output,
                InputOutputNames.get(ClassNames.WordCountJoinNr).output + "/" + InputOutputNames.outputName);
*/
        Job joinStep2 = joinWordCountNrStep2();
        if (!joinStep2.waitForCompletion(true)) {
            System.exit(1);
        }
  /*      JobBuilder.mergeOutput(InputOutputNames.get(ClassNames.WordCountJoinNrStep2).output,
                InputOutputNames.get(ClassNames.WordCountJoinNrStep2).output + "/" + InputOutputNames.outputName);

  */   Job joinTrDenominator = joinTrDenominator();
        if (!joinTrDenominator.waitForCompletion(true)) {
            System.exit(1);
        }
//        JobBuilder.mergeOutput(InputOutputNames.get(ClassNames.WordCountDenominatorJoinTr).output,
//                InputOutputNames.get(ClassNames.WordCountDenominatorJoinTr).output + "/" + InputOutputNames.outputName);

        Job joinTrDenominatorStep2 = joinTrDenominatorStep2();
        if (!joinTrDenominatorStep2.waitForCompletion(true)) {
            System.exit(1);
        }
//        JobBuilder.mergeOutput(InputOutputNames.get(ClassNames.WordCountDenominatorJoinTrStep2).output,
//                InputOutputNames.get(ClassNames.WordCountDenominatorJoinTrStep2).output + "/" + InputOutputNames.outputName);

        Job sortJob = sortJob();
        if (!sortJob.waitForCompletion(true)) {
            System.exit(1);
        }
//        JobBuilder.mergeOutput(InputOutputNames.get(ClassNames.Sort).output,
//                InputOutputNames.get(ClassNames.Sort).output + "/" + InputOutputNames.outputName);

        return;
    }

    private static Job wordCountJob() throws IOException, URISyntaxException {
        return JobBuilder.builder()
                .jarByClass(WordCount.class)
                .jobName("word count")
                .mapperClass(WordCount.MapperClass.class)
                .partitionerClass(WordCount.PartitionerClass.class)
                .combinerClass(WordCount.CombinerClass.class)
                .reducerClass(WordCount.ReducerClass.class)
                .inputPath(InputOutputNames.get(ClassNames.WordCount).inputs[0])
                .outputPath(InputOutputNames.get(ClassNames.WordCount).output)
                .cacheFile(new URI(InputOutputNames.stopWords))
                .numberOfReducers(5)
                //.mapOutputValueClass(LongWritable.class)
                .inputFormatClass(SequenceFileInputFormat.class)//FIXME: for debugging
                .build();
    }

    private static Job NrJob() throws IOException {
        return JobBuilder.builder()
                .jarByClass(Nr.class)
                .jobName("Nr")
                .mapperClass(Nr.MapperClass.class)
                .partitionerClass(Nr.PartitionerClass.class)
                .reducerClass(Nr.ReducerClass.class)
                //.combinerClass(Nr.ReducerClass.class)
                .mapOutputValueClass(LongWritable.class)
                .outputValueClass(LongWritable.class)
                .inputPath(InputOutputNames.get(ClassNames.Nr).inputs[0])
                .outputPath(InputOutputNames.get(ClassNames.Nr).output)
                .numberOfReducers(1)
                .build();
    }

    private static Job TrJob() throws IOException {
        return JobBuilder.builder()
                .jarByClass(Tr.class)
                .jobName("Tr")
                .mapperClass(Tr.MapperClass.class)
                .partitionerClass(Tr.PartitionerClass.class)
                .reducerClass(Tr.ReducerClass.class)
                .combinerClass(Tr.ReducerClass.class)
                .mapOutputValueClass(LongWritable.class)
                .outputValueClass(LongWritable.class)
                .inputPath(InputOutputNames.get(ClassNames.Tr).inputs[0])
                .outputPath(InputOutputNames.get(ClassNames.Tr).output)
                .numberOfReducers(1)
                .build();
    }

    private static Job joinWordCountNr() throws IOException {
        String[] inputs = InputOutputNames.get(ClassNames.WordCountJoinNr).inputs;
        return JobBuilder.builder()
                .jarByClass(JoinWordCountNr.class)
                .jobName("Join - {word count, Nr} ")
                .partitionerClass(JoinWordCountNr.PartitionerClass.class)
                .reducerClass(JoinWordCountNr.ReducerClass.class)
                .addInputPath(inputs[0], JoinWordCountNr.MapperClassNr.class)
                .addInputPath(inputs[1], JoinWordCountNr.MapperClassWordCount.class)
                .outputPath(InputOutputNames.get(ClassNames.WordCountJoinNr).output)
                .setVariable(N)
                .numberOfReducers(1)
                .build();
    }

    private static Job joinWordCountNrStep2() throws IOException {
        return JobBuilder.builder()
                .jarByClass(JoinWordCountNrStep2.class)
                .jobName("Join-2 - {word count, Nr} ")
                .mapperClass(JoinWordCountNrStep2.MapperClass.class)
                .partitionerClass(JoinWordCountNrStep2.PartitionerClass.class)
                .reducerClass(JoinWordCountNrStep2.ReducerClass.class)
                .inputPath(InputOutputNames.get(ClassNames.WordCountJoinNrStep2).inputs[0])
                .outputPath(InputOutputNames.get(ClassNames.WordCountJoinNrStep2).output)
                .numberOfReducers(1)
                .build();
    }

    private static Job joinTrDenominator() throws IOException {
        String[] inputs = InputOutputNames.get(ClassNames.WordCountDenominatorJoinTr).inputs;
        return JobBuilder.builder()
                .jarByClass(JoinTrDenominator.class)
                .jobName("Join - {Tr, denominator} ")
                .partitionerClass(JoinTrDenominator.PartitionerClass.class)
                .reducerClass(JoinTrDenominator.ReducerClass.class)
                .addInputPath(inputs[0], JoinTrDenominator.MapperClassTr.class)
                .addInputPath(inputs[1], JoinTrDenominator.MapperClassWordCountDenominator.class)
                .outputPath(InputOutputNames.get(ClassNames.WordCountDenominatorJoinTr).output)
                .numberOfReducers(1)
                .build();
    }

    private static Job joinTrDenominatorStep2() throws IOException {
        return JobBuilder.builder()
                .jarByClass(JoinTrDenominatorStep2.class)
                .jobName("Join-2 - {word count, Nr} ")
                .mapperClass(JoinTrDenominatorStep2.MapperClass.class)
                .partitionerClass(JoinTrDenominatorStep2.PartitionerClass.class)
                .reducerClass(JoinTrDenominatorStep2.ReducerClass.class)
                .inputPath(InputOutputNames.get(ClassNames.WordCountDenominatorJoinTrStep2).inputs[0])
                .outputPath(InputOutputNames.get(ClassNames.WordCountDenominatorJoinTrStep2).output)
                .build();
    }

    private static Job sortJob() throws IOException {
        return JobBuilder.builder()
                .jarByClass(SortJob.class)
                .jobName("sort")
                .mapperClass(SortJob.MapperClass.class)
                .partitionerClass(SortJob.PartitionerClass.class)
                .reducerClass(SortJob.ReducerClass.class)
                .inputPath(InputOutputNames.get(ClassNames.Sort).inputs[0])
                .outputPath(InputOutputNames.get(ClassNames.Sort).output)
                .numberOfReducers(1)
                .build();
    }

}
