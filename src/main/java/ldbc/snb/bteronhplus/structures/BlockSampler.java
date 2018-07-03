package ldbc.snb.bteronhplus.structures;

import org.jgrapht.graph.builder.GraphBuilder;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class BlockSampler {
    
    private int     modelId []           = null;
    private long    degreePerModelType[] = null;
    private double  cumulativeProb[]     = null;
    private Map<Integer,Long> offsets    = null;
    private long    numModels []         = null;
    private CommunityStreamer   streamer = null;
    private long    numCommunities       = 0L;
    
    
    
    public BlockSampler(Map<Integer, Long> counts,
                        CommunityStreamer streamer) {
        
        this.streamer = streamer;
        this.numModels = new long[counts.keySet().size()];
        this.degreePerModelType = new long[counts.keySet().size()];
        Arrays.fill(degreePerModelType, 0L);
        this.modelId            = new int[degreePerModelType.length];
        this.offsets = new HashMap<Integer, Long>();
        
        if(modelId.length > 0) {
    
            long currentOffset = 0;
    
            long totalExternalDegree = 0;
            int index = 0;
            for (Map.Entry<Integer, Long> entry : counts.entrySet()) {
                Community model = streamer.getModel(entry.getKey());
                modelId[index] = model.getId();
                degreePerModelType[index] += (model.getExternalDegree()) * entry.getValue();
                totalExternalDegree += (model.getExternalDegree()) * entry.getValue();
                this.offsets.put(model.getId(), currentOffset);
                this.numModels[index] = entry.getValue();
                currentOffset += model.getSize() * entry.getValue();
                numCommunities += entry.getValue();
                index++;
            }
    
            cumulativeProb = new double[degreePerModelType.length];
            cumulativeProb[0] = 0.0;
            for (int i = 1; i < cumulativeProb.length; ++i) {
                cumulativeProb[i] = cumulativeProb[i - 1] + degreePerModelType[i - 1] / (double) totalExternalDegree;
            }
            
            
        }
        
    }
    
    public long sample(Random random, long offset) {
        
        if(modelId.length > 0) {
    
            int pos = Arrays.binarySearch(cumulativeProb, random.nextDouble());
    
            if (pos < 0) {
                pos = -(pos + 1);
                pos--;
            }
    
            /*if (pos == -1) {
                pos = 0;
            }*/
    
            Community model = streamer.getModel(modelId[pos]);
            long modelOffset = offsets.get(modelId[pos]);
            long instanceIndex = random.nextLong() % numModels[pos];
            modelOffset += model.getSize() * instanceIndex;
            long finalOffset = offset + modelOffset;
            return model.sampleNode(random, finalOffset);
        }
        return -1;
    
    }
    
    public void generateConnectedGraph(FileWriter writer, Random random, long baseOffset, GraphBuilder builder) throws
        IOException {
        
        if(numModels.length > 1) {
            ArrayList<Community> models = new ArrayList<Community>();
            ArrayList<Long> modelsOffsets = new ArrayList<Long>();
            for (int i = 0; i < numModels.length; ++i) {
                Community model = streamer.getModel(modelId[i]);
                long nextOffset = baseOffset + offsets.get(modelId[i]);
                for (int j = 0; j < numModels[i]; ++j) {
                    if(model.getExternalDegree() > 0) {
                        models.add(model);
                        modelsOffsets.add(nextOffset);
                        nextOffset += models.size();
                    }
                }
            }
    
            if(models.size() > 0) {
                Community root = models.get(0);
                for (int i = 1; i < models.size(); ++i) {
                    Community nextModel = models.get(i);
                    long node1 = root.sampleNode(random, modelsOffsets.get(0));
                    long node2 = nextModel.sampleNode(random, modelsOffsets.get(i));
        
                    writer.write(node1 + "\t" + node2 + "\n");
                    if(node1 != node2)
                        builder.addEdge(node1, node2);
                }
            }
        }
        
    
    }
    
    public void generateCommunityEdges(FileWriter writer,
                                              Map<Integer, Long> counts,
                                              CommunityStreamer communityStreamer,
                                              long offset,
                                              GraphBuilder builder) throws IOException {
        
        for(Map.Entry<Integer,Long> entry : counts.entrySet()) {
            Community model = communityStreamer.getModel(entry.getKey());
            long currentOffset = offset + offsets.get(entry.getKey());
            for(int i = 0; i < entry.getValue(); ++i) {
                for(Edge edge : model.getEdges()) {
                    long tail = (currentOffset + edge.getTail());
                    long head = (currentOffset + edge.getHead());
                    writer.write( tail + "\t" + head + "\n");
                    builder.addEdge(tail,head);
                }
                currentOffset += model.getSize();
            }
        }
        
    }
    
    public long getNumCommunities() {
        return numCommunities;
    }
}
