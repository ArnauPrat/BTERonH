package ldbc.snb.bteronhplus.structures;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public interface SuperNode {
    long getSize();

    long getInternalDegree();

    long getExternalDegree();

    int getId();

    void sampleEdges(FileWriter writer, Random random, long numEdges, long offset) throws IOException;

    long sampleNode(Random random, long offset);
    
}
