package org.example.CreateJobs;


import java.util.HashMap;

enum ClassNames {
    FilterIrrelevantDependencies,
    GetRelevantDependencies, //without the pair of nouns
    CreateTrainingVectors
}

public class InputOutputNames {


    public static class InputOutput {
        public final String[] inputs;
        public final String output;

        public InputOutput(String[] inputs, String output) {
            this.inputs = inputs;
            this.output = output;
        }
    }

    //fields
    private static String[] argumentInput;
    private static final HashMap<ClassNames, InputOutput> IO_MAP = new HashMap<>();
    //public static String outputName = "part-r-00000";

    //constructor
    private InputOutputNames(String[] args) {
        /*args:
        * 0: syntactic n-gram
        * 1: hypernym
        * 2: DP_MIN
        * 3: output path
        */

        argumentInput = args;
        initializeInputOutput();
    }

    public static void init(String[] argumentInput) {
        new InputOutputNames(argumentInput);
    }

    private void initializeInputOutput() {
        String outputPrefix = argumentInput[1].substring(0, argumentInput[1].lastIndexOf("/")+1);
        String inputSuffix = "/"+outputName;//TODO: change to a new name which will be the same as the merged name

        IO_MAP.put(ClassNames.WordCount, new InputOutput(new String[]{argumentInput[0]},
                outputPrefix + ClassNames.WordCount.name() + "Output"));
        IO_MAP.put(ClassNames.Nr, new InputOutput(new String[]{get(ClassNames.WordCount).output + inputSuffix},
                outputPrefix + ClassNames.Nr.name() + "Output"));
        IO_MAP.put(ClassNames.Tr, new InputOutput(new String[]{get(ClassNames.WordCount).output + inputSuffix},
                outputPrefix + ClassNames.Tr.name() + "Output"));
        IO_MAP.put(ClassNames.WordCountJoinNr, new InputOutput(new String[]{get(ClassNames.Nr).output + inputSuffix, get(ClassNames.WordCount).output + inputSuffix},
                outputPrefix + ClassNames.WordCountJoinNr.name() + "Output"));
        IO_MAP.put(ClassNames.WordCountJoinNrStep2, new InputOutput(new String[]{get(ClassNames.WordCountJoinNr).output + inputSuffix},
                outputPrefix + ClassNames.WordCountJoinNrStep2.name() + "Output"));
        IO_MAP.put(ClassNames.WordCountDenominatorJoinTr, new InputOutput(new String[]{get(ClassNames.Tr).output + inputSuffix, get(ClassNames.WordCountJoinNrStep2).output + inputSuffix},
                outputPrefix + ClassNames.WordCountDenominatorJoinTr.name() + "Output"));
        IO_MAP.put(ClassNames.WordCountDenominatorJoinTrStep2, new InputOutput(new String[]{get(ClassNames.WordCountDenominatorJoinTr).output + inputSuffix},
                outputPrefix + ClassNames.WordCountDenominatorJoinTrStep2.name() + "Output"));
        IO_MAP.put(ClassNames.Sort, new InputOutput(new String[]{get(ClassNames.WordCountDenominatorJoinTrStep2).output + inputSuffix},
                argumentInput[1]));
    }

    public static InputOutput get(ClassNames name) {
        return IO_MAP.get(name);
    }


}
