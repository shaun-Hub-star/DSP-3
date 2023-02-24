package org.example.DependancyTree;

import kotlin.Pair;

import java.util.*;

public class Graph {
    private HashMap<Vertex, List<Vertex>> edges = new HashMap<>();
    private HashSet<Vertex> vertices = new HashSet<>();
    private HashMap<Pair<Vertex, Vertex>, String> weight = new HashMap<>();
    private String headVertexName;
    private String count;

    public Graph(String syntacticNgram) {
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
        String[] fromNodeAttributes = nodes[fromIndex].split(" ");
        Vertex from = new Vertex(fromNodeAttributes[0], fromNodeAttributes[1]);
        if (!edges.containsKey(from))
            edges.put(from, new LinkedList<>());

        edges.get(from).add(to);
        return from;
    }

    public List<String> getFullDependencyPathFromNounToNoun() {

        List<String> paths = new LinkedList<>();
        for (Map.Entry<Vertex, List<Vertex>> from : edges.entrySet()) {

            if (!from.getKey().isNoun()) continue;
            paths.add(getFullDependencyPathFromNounToNoun(from.getKey()));

        }
        return paths;
    }

    private String getFullDependencyPathFromNounToNoun(Vertex from) {
        StringBuilder path = new StringBuilder();
        for (Vertex to : edges.get(from)) {
            path.append(
                    from.getWord()).
                    append(" ").
                    append(from.getPartOfSpeech()).
                    append(" ").
                    append(weight.get(new Pair<>(from, to))).
                    append(" ").
                    append(to.getWord()).
                    append(" ").
                    append(to.getPartOfSpeech()).
                    append(" ");
            if (to.isNoun()) return path.toString();
            path.append(getFullDependencyPathFromNounToNoun(to));
        }
        return path.toString();
    }

}
