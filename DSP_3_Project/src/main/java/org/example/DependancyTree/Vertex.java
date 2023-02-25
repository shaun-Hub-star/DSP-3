package org.example.DependancyTree;

import org.tartarus.snowball.ext.PorterStemmer;

import java.util.Objects;

public class Vertex {
    private String word;
    private String partOfSpeech;
    private final PorterStemmer stemmer;

    public Vertex(String word, String partOfSpeech, PorterStemmer stemmer) {
        this.stemmer = stemmer;
        stemmer.setCurrent(word);
        stemmer.stem();
        this.word = stemmer.getCurrent();
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
