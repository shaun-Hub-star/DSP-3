package org.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;


@SuppressWarnings({"rawtypes"})

public class JobBuilder {
    private static Job job;
    private static Configuration configuration;

    private JobBuilder() {

    }

    public static JobBuilder builder() throws IOException {
        configuration = new Configuration();
      /*  String aws_access_key_id = "ASIATFAT2J2GYAQFBXXS";
        String aws_secret_access_key = "jYQG2ZzlsNfRHzXDqe/IhWQ8hQGiZdp1y+kiuNHQ";
//aws_session_token=FwoGZXIvYXdzEBcaDC7HnXrOE332MNLuFiLIAaDMnrzqFaj7Pp3teEQDR6Ng9h2l9FrFOftuY8msRi6v23cZJOeG/2Rm1Y/MFvW0+ORKLWbpkSfN9VZQLtSj8qYXQfPpKD88zNm5/cfSqVi/CX77Bec+zkFGOlj3XrdKWM1GSrPCNqlJcJdLCkrkwY8EVNLkB1XEib2n1KgFchsWIO1tlvPxx4pYgdckp6NCbJZk3OJgPGLcxccZDb+SYfaKp7cv9Grkq2rHViEpPrCus4h4wHjTF3rZld9gu9D8o+/CvjFOQ97jKLiT650GMi3nO4Ezipv0i/opAwxVHpsLHSjy1/pegsEp6dQuuhsICJkpLQ7LR5X+Z3sVW+Y=
        configuration.set("fs.s3a.access.key", aws_access_key_id);
        configuration.set("fs.s3a.secret.key", aws_secret_access_key);*/

        job = Job.getInstance(configuration);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        return new JobBuilder();
    }

    public Job build() {
        return job;
    }

    public JobBuilder jarByClass(Class<?> jarClass) {
        job.setJarByClass(jarClass);
        return this;
    }

    public JobBuilder jobName(String jobName) {
        job.setJobName(jobName);
        return this;
    }

    public JobBuilder inputPath(String inputPath) throws IOException {
        FileInputFormat.addInputPath(job, new Path(inputPath));
        return this;
    }

    public JobBuilder outputPath(String outputPath) {
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return this;
    }

    public JobBuilder mapperClass(Class<? extends Mapper> mapperClass) {
        job.setMapperClass(mapperClass);
        return this;
    }

    public JobBuilder reducerClass(Class<? extends Reducer> reducerClass) {
        job.setReducerClass(reducerClass);
        return this;
    }

    public JobBuilder combinerClass(Class<? extends Reducer> reducerClass) {
        job.setCombinerClass(reducerClass);
        return this;
    }

    public JobBuilder partitionerClass(Class<? extends Partitioner> partitionerClass) {
        job.setPartitionerClass(partitionerClass);
        return this;
    }

    public JobBuilder cacheFile(URI file) {
        job.addCacheFile(file);
        return this;
    }

    public JobBuilder mapOutputKeyClass(Class<?> mapOutputKeyClass) {
        job.setMapOutputKeyClass(mapOutputKeyClass);
        return this;
    }

    public JobBuilder mapOutputValueClass(Class<?> mapOutputValueClass) {
        job.setMapOutputValueClass(mapOutputValueClass);
        return this;
    }

    public JobBuilder outputKeyClass(Class<?> outputKeyClass) {
        job.setOutputKeyClass(outputKeyClass);
        return this;
    }

    public JobBuilder outputValueClass(Class<?> outputValueClass) {
        job.setOutputValueClass(outputValueClass);
        return this;
    }

    public JobBuilder outputFormatClass(Class<? extends OutputFormat> outputFormat) {
        job.setOutputFormatClass(outputFormat);
        return this;
    }

    public JobBuilder addInputPath(String inputPath, Class<? extends Mapper> mapperClass) {
        MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class, mapperClass);
        return this;
    }

    public JobBuilder setVariable(double N) {
        configuration.setDouble("N", N);
        return this;
    }

    public JobBuilder inputFormatClass(Class<? extends InputFormat> fileInputFormatClass) {
        job.setInputFormatClass(fileInputFormatClass);
        return this;
    }

    /*public static void mergeOutput(String pathToMerge, String newPath) throws IOException {

        FileSystem fs = FileSystem.get(configuration);

        // Input path for the getMerge function
        Path inputPath = new Path(pathToMerge);

        // Output path for the merged file
        Path outputPath = new Path(newPath);

        FileUtil.copy(fs, inputPath, fs, outputPath, true, configuration);
    }*/
    public static void mergeOutput(String srcDir, String dstFile) throws IOException {
        //FileSystem fs = S3AFileSystem.get(configuration);
        //FileUtil.copy(fs, new Path(srcDir), fs, new Path(dstFile), true, configuration);
        //FileSystem fs = FileSystem.get(configuration);
        /*FileSystem fs = S3AFileSystem.get(configuration);
        if (fs == null) throw new IOException("null");
        Path srcPath = new Path(srcDir);
        Path dstPath = new Path(dstFile);

        FileStatus[] fileStatuses = fs.listStatus(srcPath);
        List<FileStatus> fileStatusList = Arrays.asList(fileStatuses);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(dstPath)));

        for (FileStatus fileStatus : fileStatusList) {
            Path path = fileStatus.getPath();
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            while ((line = br.readLine()) != null) {
                bw.write(line);
                bw.newLine();
            }
            br.close();
        }

        bw.close();*/

    }

    public JobBuilder numberOfReducers(int numberOfReducers) {
        job.setNumReduceTasks(numberOfReducers);
        return this;
    }
}
