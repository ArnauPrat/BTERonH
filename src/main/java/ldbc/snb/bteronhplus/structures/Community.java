package ldbc.snb.bteronhplus.structures;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.jgrapht.Graph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.jgrapht.graph.builder.GraphBuilder;

public class Community implements SuperNode {
    private int            id;
    private List<Edge>     edges;
    private List<Integer>  excessDegree;
    private List<Double>    clusteringCoefficient;
    private double          excessDegreeProbs[];
    private int             excessDegreeIndices[];
    private int             internalDegree = 0;
    private int             externalDegree = 0;
    private List<Integer>  externalTriangles;
    
    public Community(int id,
                     List<Integer> excessDegree,
                     List<Double> clusteringCoefficient,
                     List<Edge> edges) {
        this.id           = id;
        this.edges        = edges;
        this.excessDegree = excessDegree;
        this.clusteringCoefficient = clusteringCoefficient;
        this.externalTriangles = new ArrayList<Integer>();
        this.internalDegree = edges.size()*2;
        this.excessDegreeIndices = new int[excessDegree.size()];
        int excessDegreeWitohutTriangles[] = new int[excessDegree.size()];
        long degreeWithoutTriangles = 0L;
    
        GraphBuilder builder = SimpleGraph.createBuilder(DefaultEdge.class);
        for(int i = 0; i < excessDegree.size(); ++i) {
            builder.addVertex(new Long(i));
        }
        
        for(Edge edge : edges) {
            builder.addEdge(edge.getTail(), edge.getHead());
        }
        
        Graph<Long, DefaultEdge> graph = builder.build();
        for(int i = 0; i < excessDegree.size(); ++i) {
            int numTriangles = 0;
            Set<DefaultEdge> neighborEdges = graph.outgoingEdgesOf(new Long(i));
            Set<Long> neighbors = new HashSet<Long>();
            for(DefaultEdge edge : neighborEdges) {
                neighbors.add(graph.getEdgeSource(edge));
                neighbors.add(graph.getEdgeTarget(edge));
            }
            neighbors.remove(new Long(i));
            
            for(Long neighbor : neighbors) {
    
                for(DefaultEdge edge2 : graph.outgoingEdgesOf(neighbor)) {
                    if(neighbors.contains(graph.getEdgeSource(edge2)) &&
                        neighbors.contains(graph.getEdgeTarget(edge2))) {
                        numTriangles++;
                    }
                }
            }
            int degree = graph.degreeOf(new Long(i)) + excessDegree.get(i);
            int missingTriangles = (int)(clusteringCoefficient.get(i)*degree*(degree-1) - numTriangles) / 2;
            externalTriangles.add(missingTriangles);
        }
    
        int next = 0;
        for(int i = 0; i < excessDegree.size(); ++i) {
            int nextDegree = excessDegree.get(i);
            this.externalDegree += nextDegree;
            nextDegree -= Math.sqrt(2.0*externalTriangles.get(i));
            excessDegreeWitohutTriangles[i] = nextDegree;
            degreeWithoutTriangles += nextDegree;
            if(nextDegree > 0) {
                excessDegreeIndices[next] = i;
                next++;
            }
        }
    
        this.excessDegreeProbs = new double[next];
        if(next > 0) {
            excessDegreeProbs[0] = 0.0;
            for (int i = 1; i < next; ++i) {
                excessDegreeProbs[i] = excessDegreeProbs[i - 1] + excessDegreeWitohutTriangles[excessDegreeIndices[i - 1]] /
                    (double)  degreeWithoutTriangles;
            }
        }
        
    }
    
    public List<Edge> getEdges() {
        return edges;
    }
    
    @Override
    public int getId() {
        return id;
    }
    
    @Override
    public long getSize() {
        return excessDegree.size();
    }
    
    @Override
    public long getInternalDegree() {
        return internalDegree;
    }
    
    @Override
    public long getExternalDegree() {
        return externalDegree;
    }
    
    public List<Integer> getExternalTriangles() {
        return externalTriangles;
    }
    
    public List<Integer> getExcessDegree() {
        return excessDegree;
    }
    
    @Override
    public void sampleEdges(FileWriter writer, Random random, long numEdges, long offset) throws IOException {
    
        for(Edge edge : edges) {
            writer.write((offset + edge.getTail()) + "\t" + (offset + edge.getHead()) + "\n");
        }
    }
    
    @Override
    public long sampleNode(Random random, long offset) {
        
        int pos = Arrays.binarySearch(excessDegreeProbs, random.nextDouble());
        
        if(pos < 0) {
            pos  = -(pos+1);
            pos--;
        }
        
        if(pos == -1) {
            pos = 0;
        }
    
        return offset + excessDegreeIndices[pos];
    }
    
}
