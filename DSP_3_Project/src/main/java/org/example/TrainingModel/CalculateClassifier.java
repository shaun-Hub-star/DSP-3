package org.example.TrainingModel;

import com.opencsv.CSVWriter;

import org.example.AWS_Services.S3Instance;
import software.amazon.awssdk.regions.Region;
import weka.classifiers.functions.LinearRegression;

import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CalculateClassifier {
    private final String trainingDataFileName;
    private final S3Instance s3Instance;
    private LinearRegression classifier;
    public static HashMap<String, Instance> nounPairToInstanceMap = new HashMap<>();
    private final List<String> nounPairs = new ArrayList<>();

    public CalculateClassifier(String bucketOfTrainingDataName, String trainingDataFileName) {
        this.trainingDataFileName = trainingDataFileName;
        this.s3Instance = new S3Instance(Region.US_EAST_1, bucketOfTrainingDataName);
    }

    /*private String downloadTXTFileIntoString(String fileName) {
        Path path = Paths.get(fileName);
        String content = null;
        try {
            content = FileUtils.readFileToString(path.toFile(), "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }*/

    public static void main(String[] args) {
        CalculateClassifier calculateClassifier = new CalculateClassifier("training-data-bucket", "TrainingData.txt");
        calculateClassifier.trainModelNew();
    }

    public void trainModelNew() {
        //<noun-pair>\t<vector>\t<label>
        String trainingData = s3Instance.downloadFileContentFromS3(trainingDataFileName, "txt", "src/main/java/org/example/TrainingModel/TrainingDataFromS3");
        //trainingData = downloadTXTFileIntoString("C:\\Users\\Shust\\OneDrive\\Documents\\First Degree\\Semester 5\\Distributed Systems\\Projects\\Assignment3\\DSP-3\\DSP_3_Project\\src\\main\\java\\org\\example\\TrainingModel\\TrainingData.txt");
        String[] trainingDataVectors = trainingData.split("\\n");
        String csvFilePath = "src/main/java/org/example/TrainingModel/InputFilesForTrainingModel/TrainingDataCSV.csv";
        String arffFilePath = "src/main/java/org/example/TrainingModel/InputFilesForTrainingModel/TrainingDataARFF.arff";

        try {
            createCVSFile(trainingDataVectors, csvFilePath);
            convertCSVToArff(csvFilePath, arffFilePath);
            Instances data = ConverterUtils.DataSource.read(arffFilePath);
            initNounPairToInstanceMap(data);
            data.setClassIndex(data.numAttributes() - 1);
            this.classifier = new LinearRegression();
            classifier.buildClassifier(data);

            //System.out.println("[DEBUG]: " + classifier);
            //print Predicted Label
            System.out.println("[DEBUG]: " + predictedLabel("1,2,3,4,5,6,7,8,9,11"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initNounPairToInstanceMap(Instances trainingDataVectors) {
        for (int i = 0; i < trainingDataVectors.numInstances(); i++) {
            Instance instance = trainingDataVectors.instance(i);
            String nounPair = this.nounPairs.get(i);
            nounPairToInstanceMap.put(nounPair, instance);
        }
    }

    private Instance convertVectorInputToInstance(String vector, double label) {
        String[] trainingDataVectorValues = vector.split(",");
        int number_of_rows = trainingDataVectorValues.length;
        double[] trainingDataVectorValuesDouble = new double[number_of_rows];
        for (int i = 0; i < number_of_rows; i++) {
            trainingDataVectorValuesDouble[i] = Double.parseDouble(trainingDataVectorValues[i]);
        }
        Instance instance = new DenseInstance(number_of_rows + 1);
        for (int j = 0; j < number_of_rows; j++) {
            instance.setValue(j, trainingDataVectorValuesDouble[j]);
        }
        if (label == 1.0 || label == 0.0)
            instance.setValue(number_of_rows, label);
        return instance;
    }

    public double predictedLabel(String vector) throws Exception {
        Instance instance = convertVectorInputToInstance(vector, -999);
        return classifier.classifyInstance(instance);
    }

    private List<String[]> convertVectorsToList(String[] trainingDataVectors) throws IOException {
        List<String[]> csvLines = new ArrayList<>();
        boolean firstLine = true;
        for (String trainingDataVector : trainingDataVectors) {
            String[] trainingDataVectorParts = trainingDataVector.split("\t");
            String noun_pair = trainingDataVectorParts[0];
            this.nounPairs.add(noun_pair);
            String[] trainingVector = trainingDataVectorParts[1].split(",");
            firstLine = createAttributes(csvLines, firstLine, trainingVector);
            String stringLabel = trainingDataVectorParts[2];
            stringLabel = stringLabel.replace("\r", "");
            String label = stringLabel.equals("True") ? "1" : "0";
            //concat the label to the end of the vector
            trainingVector = java.util.Arrays.copyOf(trainingVector, trainingVector.length + 1);
            trainingVector[trainingVector.length - 1] = label;

            csvLines.add(trainingVector);
        }
        return csvLines;

    }

    private static boolean createAttributes(List<String[]> csvLines, boolean firstLine, String[] trainingVector) {
        if (firstLine) {
            String[] attributes = new String[trainingVector.length + 1];
            for (int i = 0; i < attributes.length - 1; i++) {
                attributes[i] = "attribute" + i;
            }
            attributes[attributes.length - 1] = "label";
            csvLines.add(attributes);
        }
        return false;
    }

    private void createCVSFile(String[] trainingDataVectors, String pathToCreateTheCSV) throws IOException, URISyntaxException {
        List<String[]> csvLines = convertVectorsToList(trainingDataVectors);
        try (CSVWriter writer = new CSVWriter(new FileWriter(pathToCreateTheCSV))) {
            writer.writeAll(csvLines);
        }
    }

    private void convertCSVToArff(String pathToCSV, String pathToArff) throws IOException {
        // load CSV
        CSVLoader loader = new CSVLoader();
        loader.setSource(new File(pathToCSV));
        Instances data = loader.getDataSet();

        // save ARFF
        ArffSaver saver = new ArffSaver();
        saver.setInstances(data);
        saver.setFile(new File(pathToArff));
        saver.writeBatch();

    }


/*    private void writeToTxtFile(String fileName) throws IOException {
        FileWriter fileWriter = new FileWriter(fileName);
        for(int line = 0; line < 100; line++) {
            fileWriter.write(content);
        }
        fileWriter.close();
    }*/
}
