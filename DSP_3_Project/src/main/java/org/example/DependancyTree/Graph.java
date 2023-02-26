package org.example.DependancyTree;

import kotlin.Pair;
import org.apache.hadoop.io.Text;
import org.tartarus.snowball.ext.PorterStemmer;

import java.util.*;
import java.util.stream.Collectors;

public class Graph {
    private final HashMap<Vertex, List<Vertex>> edges = new HashMap<>();
    private final HashSet<Vertex> vertices = new HashSet<>();
    private final HashMap<Pair<Vertex, Vertex>, String> weight = new HashMap<>();
    private String headVertexName;
    private final String count;
    private final PorterStemmer stemmer;

    public static void main(String[] args) {
        String syntacticNgram = "queue\tkg/lb/7.7/Y/NNP/dep/2 l/POS/dep/3 queue/NN/pobj/1\t18\t1980,1\t1988,2\t1997,6\t1999,2\t2002,4\t2007,3\n";
        Graph g = new Graph(syntacticNgram);
        for (String path : g.getFullDependencyPathFromNounToNoun()) {
            if (path == null) break;
            String[] splitBySpace = path.split(" ");
            String firstWord = splitBySpace[0];
            String lastWord = splitBySpace[splitBySpace.length - 2];
            String start = splitBySpace[0] + splitBySpace[1];
            int trimStartIndex = start.length() + 2;
            String end = splitBySpace[splitBySpace.length - 1] + splitBySpace[splitBySpace.length - 2];
            int trimEndIndex = path.length() - end.length() - 2;
            String pathWithoutStringAndEnd = path.substring(trimStartIndex, trimEndIndex);

            System.out.println("key: " + pathWithoutStringAndEnd);
            System.out.println("value: " + firstWord + " " + lastWord + "\t" + g.getCount());

        }
    }


    //Graph g = new Graph(syntacticNgram);
    //System.out.println(Arrays.toString(g.getNodeAttributes("kg/lb/7.7/Y//NNP/dep/2")));
    //System.out.println(g.getFullDependencyPathFromNounToNoun());
        /*for (String path : g.getFullDependencyPathFromNounToNoun()) {
            String[] splitBySpace = path.split(" ");
            String start = splitBySpace[0] + splitBySpace[1];
            int trimStartIndex = start.length() + 2;
            String end = splitBySpace[splitBySpace.length - 1] + splitBySpace[splitBySpace.length - 2];
            int trimEndIndex = path.length() - end.length() - 2;
            String pathWithoutStringAndEnd = path.substring(trimStartIndex, trimEndIndex);
            System.out.println(pathWithoutStringAndEnd);
            //context.write(new Text(pathWithoutStringAndEnd), new Text(start + " " + end + "\t" + g.getCount()));
        }*/

    //write me a function that returns a string[] of the last k last slashes


    private String[] splitLastSlash(String line) {
        int index = line.lastIndexOf('/');
        if (index == -1)
            return new String[]{"", line};

        String left = line.substring(0, index);
        String right = line.substring(index + 1);
        return new String[]{left, right};
    }

    private String[] getNodeAttributes(String line) {
        int k = 3;
        String[] parts = new String[k + 1];
        return splitByLastKSlashesHelper(line, k, parts, 0);
    }

    private String[] splitByLastKSlashesHelper(String line, int k, String[] parts, int i) {
        if (i == k) {
            parts[0] = line;
            return parts;
        }

        parts[k - i] = splitLastSlash(line)[1];
        line = splitLastSlash(line)[0];
        return splitByLastKSlashesHelper(line, k, parts, i + 1);
    }


    public Graph(String syntacticNgram) {
        this.stemmer = new PorterStemmer();
        //split by tab
        String[] parts = syntacticNgram.split("\\t");
        headVertexName = parts[0];
        count = parts[2];
        String[] nodes = parts[1].split(" ");
        String[] sortedNodes = sortByIndex(nodes);

        if (nodes.length == 0) System.out.println("corrupted dependency?\t" + syntacticNgram);

        for (String node : sortedNodes) {
            String[] nodeAttributes = getNodeAttributes(node);

            //vertices
            Vertex to = new Vertex(nodeAttributes[0], nodeAttributes[1], stemmer);
            vertices.add(to);

            //edges
            Vertex from = addFromEdge(nodes, nodeAttributes, to);

            //weight
            weight.put(new Pair<>(from, to), nodeAttributes[2]);
        }
    }

    public String getCount() {
        return count;
    }

    private String[] sortByIndex(String[] nodes) {
        try {
            nodes = Arrays.stream(nodes).sorted(
                    Comparator.comparingInt(node -> Integer.parseInt(node.substring(node.lastIndexOf('/') + 1)))
            ).toArray(String[]::new);
        } catch (Exception e) {
            return new String[]{};
        }

        return nodes;
    }

    private Vertex addFromEdge(String[] nodes, String[] nodeAttributes, Vertex to) {
        int fromIndex = Integer.parseInt(nodeAttributes[3]) - 1;
        if (fromIndex == -1) return to;
        String[] fromNodeAttributes = nodes[fromIndex].split("/");
        Vertex from = new Vertex(fromNodeAttributes[0], fromNodeAttributes[1], stemmer);
        if (!edges.containsKey(from))
            edges.put(from, new LinkedList<>());

        edges.get(from).add(to);
        return from;
    }

    public List<String> getFullDependencyPathFromNounToNoun() {

        List<String> paths = new LinkedList<>();
        for (Vertex from : vertices) {

            if (!from.isNoun()) continue;
            paths.addAll(getFullDependencyPathFromNounToNoun(from));

        }
        return paths;
    }

    private List<String> getFullDependencyPathFromNounToNoun(Vertex from) {
        List<String> paths = new LinkedList<>();
        for (Vertex to : edges.getOrDefault(from, new ArrayList<>())) {
            StringBuilder path = new StringBuilder();
            path.append(
                            from.getWord())
                    .append(" ")
                    .append(from.getPartOfSpeech())
                    .append(" ")
                    .append(weight.get(new Pair<>(from, to)));
            if (to.isNoun()) {
                path.append(" ")
                        .append(to.getWord())
                        .append(" ")
                        .append(to.getPartOfSpeech());
                paths.add(path.toString());
            } else
                paths.addAll(getFullDependencyPathFromNounToNoun(to).stream().map(p -> path + " " + p).collect(Collectors.toList()));
        }
        return paths;
    }
    /*private List<String> getFullDepenedencyPathFrom(Vertex from){
        List<List<String>> paths = new LinkedList<>();
        for (Vertex to : edges.getOrDefault(from,new ArrayList<>())) {
            paths.add(getFullDepenedencyPathFrom(from));
        }
        int shortestLength = paths.stream().map(List::size).min(Integer::compareTo).get();
        List<String> shortestPath = paths.stream().filter(path -> path.size() == shortestLength).findFirst().get();
        String current = from.getWord()+" "+from.getPartOfSpeech();
        return shortestPath.add(0,current);
    }*/
}
