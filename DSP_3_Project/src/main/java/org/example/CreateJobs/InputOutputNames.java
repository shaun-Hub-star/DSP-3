package org.example.CreateJobs;


import java.util.Arrays;
import java.util.HashMap;

public enum ClassNames {
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
    private static MainArgs argumentInput;
    private static final HashMap<ClassNames, InputOutput> IO_MAP = new HashMap<>();
    //public static String outputName = "part-r-00000";

    private final String inputSuffix = ""; //FIXME? assumes that when given a directory, the map reduce procedure will run on all files within that directory
    private final String outputPrefix;

    //constructor
    private InputOutputNames(MainArgs args) {
        argumentInput = args;
        outputPrefix = finalOutput().substring(0, finalOutput().lastIndexOf("/")+1);
        initializeInputOutput();
    }

    public static void init(MainArgs argumentInput) {
        new InputOutputNames(argumentInput);
    }

    private void initializeInputOutput() {
        addStep(ClassNames.FilterIrrelevantDependencies, syntacticNgram());
        addStep(ClassNames.GetRelevantDependencies, ClassNames.FilterIrrelevantDependencies);
        addStep(ClassNames.CreateTrainingVectors, ClassNames.FilterIrrelevantDependencies, hypernym(), ClassNames.GetRelevantDependencies);
    }

    private void addStep(ClassNames stepName, Object... inputs){
        addStep(stepName, "" , outputPrefix, stepName.name() + "Output", (String[]) Arrays.stream(inputs).map(input -> {
            if(input instanceof ClassNames)
                return get((ClassNames) input).output + inputSuffix;
            else return (String) input;
        }).toArray());
    }

    //FIXME? can create a bug in a later version, when there is a need to specify the output.
    @Deprecated
    private void addStep(ClassNames stepName, String output, ClassNames prevStep, ClassNames... prevSteps){
        String[] inputs = new String[prevSteps.length + 1];
        inputs[0] = get(prevStep).output;
        for(int i = 0; i < prevSteps.length; i++){
            inputs[i+1] = get(prevSteps[i]).output;
        }
        addStep(stepName, inputSuffix, "", output, inputs);
    }

    private void addStep(ClassNames stepName, String inputSuffix, String outputPrefix, String output, String... inputs){
        IO_MAP.put(stepName, new InputOutput((String[]) Arrays.stream(inputs).map(in -> in + inputSuffix).toArray(), outputPrefix + output));
    }

    public static InputOutput get(ClassNames name) {
        return IO_MAP.get(name);
    }

    public static String syntacticNgram(){
        return argumentInput.getSyntacticNgramPath();
    }

    public static String hypernym(){
        return argumentInput.getHypernymPath();
    }

    public static String DP_MIN(){
        return argumentInput.getDpMinPath();
    }

    public static String finalOutput(){
        return argumentInput.getOutputPath();
    }


}
