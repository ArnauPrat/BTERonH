package ldbc.snb.bteronhplus.structures;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Community implements SuperNode {
    private int         id;
    private int         numNodes;
    private List<Edge> edges;
    private int         excessDegree[];
    private double      excessDegreeProbs[];
    private int         excessDegreeIndices[];
    private int         internalDegree = 0;
    private int         externalDegree = 0;
    
    public Community(int id, int numNodes, List<Edge> edges, int excessDegree[]) {
        this.id           = id;
        this.numNodes     = numNodes;
        this.edges        = edges;
        this.excessDegree = excessDegree;
        this.internalDegree = edges.size()*2;
        this.excessDegreeIndices = new int[numNodes];
        int next = 0;
        for(int i = 0; i < excessDegree.length; ++i) {
            this.externalDegree += excessDegree[i];
            if(excessDegree[i] > 0) {
                excessDegreeIndices[next] = i;
                next++;
            }
        }
    
        this.excessDegreeProbs = new double[next];
        if(next > 0) {
            excessDegreeProbs[0] = 0.0;
            for (int i = 1; i < next; ++i) {
                excessDegreeProbs[i] = excessDegreeProbs[i - 1] + excessDegree[excessDegreeIndices[i - 1]] / (double)
                    externalDegree;
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
        return numNodes;
    }
    
    @Override
    public long getInternalDegree() {
        return internalDegree;
    }
    
    @Override
    public long getExternalDegree() {
        return externalDegree;
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
