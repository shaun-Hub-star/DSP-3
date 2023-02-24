package org.example.TrainingModel;

import com.opencsv.CSVWriter;
import org.example.AWS_Services.S3Instance;
import software.amazon.awssdk.regions.Region;
import weka.classifiers.functions.Logistic;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CalculateClassifier {
    private final String trainingDataFileName;
    private final S3Instance s3Instance;
    private Logistic classifier;
    String noun_pair;

    public CalculateClassifier(String bucketOfTrainingDataName, String trainingDataFileName) {
        this.trainingDataFileName = trainingDataFileName;
        this.s3Instance = new S3Instance(Region.US_EAST_1, bucketOfTrainingDataName);
    }

    @Deprecated
    private void trainModel() throws Exception {
        String trainingData = s3Instance.downloadFileContentFromS3(trainingDataFileName, "txt", "src/main/java/org/example/TrainingModel/TrainingDataFromS3");
        String[] trainingDataVectors = trainingData.split("\\n");
        int number_of_columns = trainingDataVectors.length;
        ArrayList<Attribute> attributes = new ArrayList<>();
        for (int i = 0; i < number_of_columns; i++) {
            attributes.add(new Attribute("column" + i));
        }
        Instances dataset = new Instances("dataset", attributes, 10000);


        for (String trainingDataVector : trainingDataVectors) {
            String[] trainingDataVectorParts = trainingDataVector.split("\t");
            this.noun_pair = trainingDataVectorParts[0];
            String vector = trainingDataVectorParts[1];
            String stringLabel = trainingDataVectorParts[2];
            double label = stringLabel.equals("True") ? 1.0 : 0.0;
            Instance instance = convertVectorInputToInstance(vector, label);
            dataset.add(instance);
        }
        dataset.setClassIndex(dataset.numAttributes() - 1);
        this.classifier = new Logistic();

        // Train classifier
        classifier.buildClassifier(dataset);


    }

    public void trainModelNew() {
        String trainingData = s3Instance.downloadFileContentFromS3(trainingDataFileName, "txt", "src/main/java/org/example/TrainingModel/TrainingDataFromS3");
        String[] trainingDataVectors = trainingData.split("\\n");
        String csvFilePath = "src/main/java/org/example/TrainingModel/InputFilesForTrainingModel/TrainingDataCSV.csv";
        String arffFilePath = "src/main/java/org/example/TrainingModel/InputFilesForTrainingModel/TrainingDataARFF.arff";

        try {
            createCVSFile(trainingDataVectors, csvFilePath);
            convertCSVToArff(csvFilePath, arffFilePath);
            Instances data = ConverterUtils.DataSource.read(arffFilePath);
            data.setClassIndex(data.numAttributes() - 1);
            this.classifier = new Logistic();
            classifier.buildClassifier(data);
            System.out.println("[DEBUG]: " + classifier);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
        for (String trainingDataVector : trainingDataVectors) {
            String[] trainingDataVectorParts = trainingDataVector.split("\t");
            this.noun_pair = trainingDataVectorParts[0];
            String[] trainingVector = trainingDataVectorParts[1].split(",");
            String stringLabel = trainingDataVectorParts[2];
            String label = stringLabel.equals("True") ? "1.0" : "0.0";
            //concat the label to the end of the vector
            trainingVector = java.util.Arrays.copyOf(trainingVector, trainingVector.length + 1);
            trainingVector[trainingVector.length - 1] = label;

            csvLines.add(trainingVector);
        }
        return csvLines;

    }

    private void writeVectorsToCSV(String[] trainingDataVectors, Path path) throws IOException {
        List<String[]> csvLines = convertVectorsToList(trainingDataVectors);
        try (CSVWriter writer = new CSVWriter(new FileWriter(path.toString()))) {
            writer.writeAll(csvLines);
        }
    }

    private void createCVSFile(String[] trainingDataVectors, String pathToCreateTheCSV) throws IOException, URISyntaxException {
        Path path = Paths.get(
                ClassLoader.getSystemResource(pathToCreateTheCSV).toURI()
        );
        writeVectorsToCSV(trainingDataVectors, path);
    }

    private void convertCSVToArff(String pathToCSV, String pathToArff) throws Exception {
        String[] options = new String[4];
        options[0] = "-T";
        options[1] = pathToCSV;
        options[2] = "-N";
        options[3] = pathToArff;
        CSVLoader.main(options);
    }

}
