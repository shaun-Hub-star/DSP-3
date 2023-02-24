package org.example.DependancyTree;

import kotlin.Pair;
import org.apache.hadoop.io.Text;

import java.util.*;
import java.util.stream.Collectors;

public class Graph {
    private HashMap<Vertex, List<Vertex>> edges = new HashMap<>();
    private HashSet<Vertex> vertices = new HashSet<>();
    private HashMap<Pair<Vertex, Vertex>, String> weight = new HashMap<>();
    private String headVertexName;
    private String count;

    public static void main(String[] args) {
        String syntacticNgram = "chair\tchair/NN/ROOT/0 is/IN/compl/1 furniture/NN/ccomp/2 wood/NN/acomp/1\t1864";
        Graph g = new Graph(syntacticNgram);
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
    }

    public Graph(String syntacticNgram) {
        //split by tab
        String[] parts = syntacticNgram.split("\\t");
        headVertexName = parts[0];
        count = parts[2];
        String[] nodes = parts[1].split(" ");
        nodes = sortByIndex(nodes);
        for (String node : nodes) {
            String[] nodeAttributes = node.split("/");
            //vertices
            Vertex to = new Vertex(nodeAttributes[0], nodeAttributes[1]);
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
        nodes = Arrays.stream(nodes).sorted(
                Comparator.comparingInt(node -> Integer.parseInt(node.split("/")[3]))
        ).toArray(String[]::new);
        return nodes;
    }

    private Vertex addFromEdge(String[] nodes, String[] nodeAttributes, Vertex to) {
        int fromIndex = Integer.parseInt(nodeAttributes[3]) - 1;
        if (fromIndex == -1) return to;
        String[] fromNodeAttributes = nodes[fromIndex].split("/");
        Vertex from = new Vertex(fromNodeAttributes[0], fromNodeAttributes[1]);
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
            }
            else
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
