package org.example.TrainingModel;

import org.example.AWS_Services.S3Instance;
import software.amazon.awssdk.regions.Region;
import weka.classifiers.functions.Logistic;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

import java.util.ArrayList;

public class CalculateClassifier {
    private final String bucketOfTrainingDataName;
    private final String trainingDataFileName;
    private S3Instance s3Instance;
    private Logistic classifier;
    String noun_pair;

    public CalculateClassifier(String bucketOfTrainingDataName, String trainingDataFileName) {
        this.bucketOfTrainingDataName = bucketOfTrainingDataName;
        this.trainingDataFileName = trainingDataFileName;
        this.s3Instance = new S3Instance(Region.SA_EAST_1, bucketOfTrainingDataName);
    }

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
            String[] trainingDataVectorParts = trainingDataVector.split("\\t");
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


}
