package org.example.CreateJobs;

public class MainArgs {

    private static String syntacticNgramPath;
    private static String hypernymPath;
    private static int DpMin = 5; //5 by default
    private static String outputPath;


    public MainArgs(String[] args){
        /*args:
         * 0: syntactic n-gram
         * 1: hypernym
         * 2: DP_MIN
         * 3: output path
         */

        if(args.length != 4)
            throw new IllegalArgumentException("args[] = {syntactic n-gram, hypernym, DP_MIN, output path})");

        syntacticNgramPath = args[0];
        hypernymPath = args[1];
        DpMin = Integer.parseInt(args[2]);
        outputPath = args[3];
    }

    public static String getSyntacticNgramPath() {
        return syntacticNgramPath;
    }

    public static String getHypernymPath() {
        return hypernymPath;
    }

    public static int getDpMin() {
        return DpMin;
    }

    public static String getOutputPath() {
        return outputPath;
    }

    public static String getOutputBucket(){
        String pref = "s3://";
        int len = pref.length();
        return outputPath.substring(len).split("/")[0];
    }
}
