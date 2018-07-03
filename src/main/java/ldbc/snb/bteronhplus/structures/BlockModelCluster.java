package ldbc.snb.bteronhplus.structures;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/*
public class BlockModelCluster implements SuperNode {
    
    private List<SuperNode> children = null;
    private BlockModel      childrenModel = null;
    private List<Integer>   childrenBlocks = null;
    private long            id = 0;
    private double          sampleProbabilityExternal[] = null;
    private long            size = 0L;
    
    private ArrayList<Entry> entries;
    
    private class Entry {
        public Entry(long id1, long id2, double prob) {
            this.id1 = id1;
            this.id2 = id2;
            this.prob = prob;
        }
        public long id1;
        public long id2;
        public double prob;
    }
    
    
    
    public BlockModelCluster(long id, List<Long> childrenBlocks, BlockModel childrenModel, List<SuperNode> children) {
        this.childrenModel = childrenModel;
        this.childrenBlocks = childrenBlocks;
        this.children = children;
        this.id = id;
        this.entries = new ArrayList<Entry>();
    
        Set<Long> availableChildren = new HashSet<Long>();
        for(SuperNode child : children) {
            availableChildren.add(child.getId());
            this.size += child.getSize();
        }
        
        Set<Long> expectedChildren = new HashSet<Long>(childrenBlocks);
        
        Map<Long, Integer> indices = new HashMap<Long,Integer>();
        for(int i = 0; i < children.size(); ++i) {
            indices.put(children.get(i).getId(), i);
        }
        
        double externalDegree[] = new double[children.size()];
        Arrays.fill(externalDegree, 0.0);
        
        double totalDegree = 0.0;
        double totalInternalDegree = 0.0;
        for(Long child : expectedChildren) {
            BlockModel.ModelEntry entry = childrenModel.getEntries().get(child);
            totalDegree += entry.totalDegree;
            Integer index = indices.get(child);
            for(Map.Entry<Long,Double> neighbor : entry.degree.entrySet()) {
                if(expectedChildren.contains(neighbor.getKey())) {
                    totalInternalDegree += neighbor.getValue();
                    if(entry.id <= neighbor.getKey()) {
                        entries.add(new Entry(entry.id, neighbor.getKey(), neighbor.getValue()));
                    }
                } else {
                    if (index != null) {
                        externalDegree[index] += neighbor.getValue();
                    }
                }
            }
        }
        
        for(Entry entry : entries) {
            entry.prob /= totalInternalDegree;
        }
        
        double totalExternalDegree = 0.0;
        for(int i = 0; i < externalDegree.length; ++i) {
            totalExternalDegree += externalDegree[i];
        }
    
    
        sampleProbabilityExternal = new double[children.size()];
        Arrays.fill(sampleProbabilityExternal, 0.0);
        sampleProbabilityExternal[0] = 0.0;
        for(int i = 1; i < sampleProbabilityExternal.length; ++i) {
            sampleProbabilityExternal[i] = sampleProbabilityExternal[i-1] + externalDegree[i] / totalExternalDegree;
        }
        
        
    }
    
    
    @Override
    public long getSize() {
        return 0;
    }
    
    @Override
    public long getInternalDegree() {
        return 0;
    }
    
    @Override
    public long getExternalDegree() {
        return 0;
    }
    
    @Override
    public int getId() {
        return 0;
    }
    
    @Override
    public void sampleEdges(FileWriter writer, Random random, long numEdges, long offset) throws IOException {
    
    }
    
    @Override
    public long sampleNode(Random random, long offset) {
        return 0;
    }
}
    */
