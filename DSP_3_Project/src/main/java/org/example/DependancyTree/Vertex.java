package org.example.DependancyTree;

import java.util.Objects;

public class Vertex {
    private String word;
    private String partOfSpeech;

    public Vertex(String word, String partOfSpeech) {
        this.word = word;
        this.partOfSpeech = partOfSpeech;
    }
    public String getWord() {
        return word;
    }
    public boolean isNoun() {
        return partOfSpeech.startsWith("NN");
    }

    public String toString() {
        return word + "/" + partOfSpeech;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Vertex vertex = (Vertex) o;
        return word.equals(vertex.word) && partOfSpeech.equals(vertex.partOfSpeech);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word, partOfSpeech);
    }

    public String getPartOfSpeech() {
        return partOfSpeech;
    }
}
