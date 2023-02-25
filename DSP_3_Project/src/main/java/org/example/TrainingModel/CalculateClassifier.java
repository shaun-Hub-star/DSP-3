package org.example.TrainingModel;

import com.opencsv.CSVWriter;

import org.example.AWS_Services.S3Instance;
import software.amazon.awssdk.regions.Region;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.functions.LinearRegression;
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
import java.util.Map;

public class CalculateClassifier {
    private final String trainingDataFileName;
    private final S3Instance s3Training;
    //private final S3Instance s3ModelOutput;
    private LinearRegression classifier;
    private final HashMap<String, Instance> nounPairToInstanceMap = new HashMap<>();
    private final List<String> nounPairs = new ArrayList<>();
    private int classIndex;
    private Evaluation eval;


    public CalculateClassifier(String bucketOfTrainingDataName, String trainingDataFileName) {
        this.trainingDataFileName = trainingDataFileName;
        this.s3Training = new S3Instance(Region.US_EAST_1, bucketOfTrainingDataName);
        //this.s3ModelOutput = new S3Instance(Region.US_EAST_1, "model-output-bucket", true);
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

    public void trainModelNew() {
        //<noun-pair>\t<vector>\t<label= {True\False}>
        String trainingData = s3Training.downloadFileContentFromS3(trainingDataFileName, "txt", "src/main/java/org/example/TrainingModel/TrainingDataFromS3");
        //trainingData = downloadTXTFileIntoString("C:\\Users\\Shust\\OneDrive\\Documents\\First Degree\\Semester 5\\Distributed Systems\\Projects\\Assignment3\\DSP-3\\DSP_3_Project\\src\\main\\java\\org\\example\\TrainingModel\\TrainingData.txt");
        String[] trainingDataVectors = trainingData.split("\\n");
        String csvFilePath = "src/main/java/org/example/TrainingModel/InputFilesForTrainingModel/TrainingDataCSV.csv";
        String arffFilePath = "src/main/java/org/example/TrainingModel/InputFilesForTrainingModel/TrainingDataARFF.arff";

        try {
            createCVSFile(trainingDataVectors, csvFilePath);
            convertCSVToArff(csvFilePath, arffFilePath);
            Instances data = ConverterUtils.DataSource.read(arffFilePath);
            initNounPairToInstanceMap(data);
            classIndex = data.numAttributes() - 1;
            data.setClassIndex(classIndex);
            this.classifier = new LinearRegression();
            //classifier.buildClassifier(data);

            eval = new Evaluation(data);
            eval.crossValidateModel(classifier, data, 10, new java.util.Random(42069));


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void modelStats() {
        double fMeasure = eval.fMeasure(classIndex);
        double recall = eval.recall(classIndex);
        double precision = eval.precision(classIndex);

        List<String> nounPairsTP = new ArrayList<>();
        List<String> nounPairsFP = new ArrayList<>();
        List<String> nounPairsTN = new ArrayList<>();
        List<String> nounPairsFN = new ArrayList<>();

        initNounPairsLists(nounPairsTP, nounPairsFP, nounPairsTN, nounPairsFN);
        createHTMLFile(fMeasure, recall, precision, nounPairsTP, nounPairsFP, nounPairsTN, nounPairsFN);
        uploadHTMLFileToS3();
    }

    private void uploadHTMLFileToS3() {
        String htmlFilePath = "src/main/java/org/example/TrainingModel/HTMLOutput/ModelStats.html";

        this.s3Training.uploadFile("ModelStats.html", htmlFilePath);

    }

    private void initNounPairsLists(List<String> nounPairsTP, List<String> nounPairsFP, List<String> nounPairsTN, List<String> nounPairsFN) {
        for (Map.Entry<String, Instance> nounVector : this.nounPairToInstanceMap.entrySet()) {
            if (nounPairsTP.size() == 5 && nounPairsFP.size() == 5 && nounPairsTN.size() == 5 && nounPairsFN.size() == 5) {
                break;
            }
            String nounPair = nounVector.getKey();
            String actual = nounVector.getValue().toString(classIndex);
            try {
                double predicted = this.predictedLabel(nounPair);
                if (nounPairsTP.size() < 5 && actual.equals("1.0") && predicted == 1.0) {
                    nounPairsTP.add(nounPair);
                } else if (nounPairsFN.size() < 5 && actual.equals("1.0") && predicted == 0.0) {
                    nounPairsFN.add(nounPair);
                } else if (nounPairsFP.size() < 5 && actual.equals("0.0") && predicted == 1.0) {
                    nounPairsFP.add(nounPair);
                } else if (nounPairsTN.size() < 5 && actual.equals("0.0") && predicted == 0.0) {
                    nounPairsTN.add(nounPair);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    private void createHTMLFile(double fMeasure, double recall, double precision, List<String> nounPairsTP, List<String> nounPairsFP, List<String> nounPairsTN, List<String> nounPairsFN) {
        String htmlFilePath = "src/main/java/org/example/TrainingModel/HTMLOutput/ModelStats.html";
        try {
            FileWriter fileWriter = new FileWriter(htmlFilePath);
            fileWriter.write("<html>\n" +
                    "<head>\n" +
                    "    <title>Model Stats</title>\n" +
                    "</head>\n" +
                    "<body>\n" +
                    "    <h1>Model Stats</h1>\n" +
                    "    <h2>True Positives</h2>\n" +
                    "    <ul>\n");
            for (String nounPair : nounPairsTP) {
                fileWriter.write("        <li>" + nounPair + "</li>\n");
            }
            fileWriter.write("    </ul>\n" +
                    "    <h2>False Positives</h2>\n" +
                    "    <ul>\n");
            for (String nounPair : nounPairsFP) {
                fileWriter.write("        <li>" + nounPair + "</li>\n");
            }
            fileWriter.write("    </ul>\n" +
                    "    <h2>True Negatives</h2>\n" +
                    "    <ul>\n");
            for (String nounPair : nounPairsTN) {
                fileWriter.write("        <li>" + nounPair + "</li>\n");
            }
            fileWriter.write("    </ul>\n" +
                    "    <h2>False Negatives</h2>\n" +
                    "    <ul>\n");
            for (String nounPair : nounPairsFN) {
                fileWriter.write("        <li>" + nounPair + "</li>\n");
            }
            fileWriter.write("    </ul>\n" +
                    "    <h2>Model Stats</h2>\n" +
                    "    <ul>\n" +
                    "        <li>F-Measure: " + fMeasure + "</li>\n" +
                    "        <li>Recall: " + recall + "</li>\n" +
                    "        <li>Precision: " + precision + "</li>\n" +
                    "    </ul>\n" +
                    "</body>\n" +
                    "</html>");
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void initNounPairToInstanceMap(Instances trainingDataVectors) {
        for (int i = 0; i < trainingDataVectors.numInstances(); i++) {
            Instance instance = trainingDataVectors.instance(i);
            String nounPair = this.nounPairs.get(i);
            nounPairToInstanceMap.put(nounPair, instance);
        }
    }

    public double predictedLabel(String nounPair) throws Exception {
        Instance instance = nounPairToInstanceMap.get(nounPair);
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
            String label = stringLabel.equals("True") ? "1.0" : "0.0";
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

}
