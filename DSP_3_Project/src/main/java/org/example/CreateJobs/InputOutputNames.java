package org.example.CreateJobs;


import jdk.jfr.internal.tool.Main;

import java.util.Arrays;
import java.util.HashMap;

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
    private static final HashMap<ClassNames, InputOutput> IO_MAP = new HashMap<>();
    //public static String outputName = "part-r-00000";

    private final String inputSuffix = ""; //FIXME? assumes that when given a directory, the map reduce procedure will run on all files within that directory
    private final String outputPrefix;

    //constructor
    private InputOutputNames() {
        outputPrefix = MainArgs.getOutputPath().substring(0, MainArgs.getOutputPath().lastIndexOf("/")+1);
        initializeInputOutput();
    }

    public static void init() {
        new InputOutputNames();
    }

    private void initializeInputOutput() {

        addStep(ClassNames.FilterIrrelevantDependencies, MainArgs.getSyntacticNgramPath());
        addStep(ClassNames.GetRelevantDependencies, ClassNames.FilterIrrelevantDependencies);
        addStep(MainArgs.getOutputPath(), ClassNames.CreateTrainingVectors, ClassNames.FilterIrrelevantDependencies, MainArgs.getHypernymPath(), ClassNames.GetRelevantDependencies);
        //DEBUG STEP
        addStep(ClassNames.DEBUG_MAP_REDUCE, MainArgs.getSyntacticNgramPath());
    }
    /*
    * main(String[]args):
    *   output_dir = args[2]
    *   map_reduce1(input_dir, output_dir + "/filterIrrelevantDependencies")
    *   map_reduce2(output_dir + "/filterIrrelevantDependencies", output_dir + "/getRelevantDependencies")
    *   map_reduce3(output_dir + "/getRelevantDependencies", output_dir + "/createTrainingVectors")
    * */
    private void addStep(ClassNames stepName, Object... inputs){
        addStep(outputPrefix + stepName.name() + "Output", stepName, inputs);
    }

    private void addStep(String output, ClassNames stepName, Object... inputs){
        String[] strInputs = new String[inputs.length];
        for(int i = 0; i < inputs.length; i++){
            if(inputs[i] instanceof ClassNames)
                strInputs[i] = get((ClassNames) inputs[i]).output + inputSuffix;
            else
                strInputs[i] = inputs[i].toString();
        }
        addStep(output, stepName, strInputs);
    }

    private void addStep(String output, ClassNames stepName, String... inputs){
        IO_MAP.put(stepName, new InputOutput(inputs, output));
    }

    public static InputOutput get(ClassNames name) {
        return IO_MAP.get(name);
    }
}
