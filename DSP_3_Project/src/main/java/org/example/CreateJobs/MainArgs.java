package org.example.CreateJobs;

public class MainArgs {

    private final String syntacticNgramPath;
    private final String hypernymPath;
    private final String DpMinPath;
    private final String outputPath;


    public MainArgs(String[] args){
        /*args:
         * 0: syntactic n-gram
         * 1: hypernym
         * 2: DP_MIN
         * 3: output path
         */

        if(args.length != 4)
            throw new IllegalArgumentException("args[] = {syntactic n-gram, hypernym, DP_MIN, output path})");

        this.syntacticNgramPath = args[0];
        this.hypernymPath = args[1];
        this.DpMinPath = args[2];
        this.outputPath = args[3];
    }

    public String getSyntacticNgramPath() {
        return syntacticNgramPath;
    }

    public String getHypernymPath() {
        return hypernymPath;
    }

    public String getDpMinPath() {
        return DpMinPath;
    }

    public String getOutputPath() {
        return outputPath;
    }
}
