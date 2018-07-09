package ldbc.snb.bteronhplus.structures;

import org.apache.commons.math3.util.Pair;
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
    private Map<Integer,Long> counts     = null;
    
    
    
    public BlockSampler(Map<Integer, Long> counts,
                        CommunityStreamer streamer) {
        
        this.counts = counts;
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
                degreePerModelType[index] = model.getExternalDegree() * entry.getValue();
                totalExternalDegree += degreePerModelType[index];
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
            ArrayList<Pair<Community, Long>> models = new ArrayList<Pair<Community,Long>>();
            for (int i = 0; i < numModels.length; ++i) {
                Community model = streamer.getModel(modelId[i]);
                long nextOffset = baseOffset + offsets.get(modelId[i]);
                for (int j = 0; j < numModels[i]; ++j) {
                    if(model.getExternalDegree() > 0) {
                        models.add(new Pair<Community,Long>(model, nextOffset));
                        nextOffset += models.size();
                    }
                }
            }
            
            models.sort(new Comparator<Pair<Community,Long>>() {
    
                @Override
                public int compare(Pair<Community, Long> pair1, Pair<Community, Long> pair2) {
                    long externalDegree1 = pair1.getFirst().getExternalDegree();
                    long externalDegree2 = pair2.getFirst().getExternalDegree();
                    if(externalDegree1 > externalDegree2) return -1;
                    if(externalDegree1 == externalDegree2) return 0;
                    return 1;
                }
            });
            
            
            
    
            if(models.size() > 0) {
                Community root = models.get(0).getFirst();
                for (int i = 1; i < models.size(); ++i) {
                    Community nextModel = models.get(i).getFirst();
                    long node1 = root.sampleNode(random, models.get(0).getSecond());
                    long node2 = nextModel.sampleNode(random, models.get(i).getSecond());
        
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
    
    private class NodeStats{
        public long nodeId;
        public int numTriangles;
        public int degree;
        
        public NodeStats(long nodeId, int numTriangles, int degree) {
            this.nodeId = nodeId;
            this.numTriangles = numTriangles;
            this.degree = degree;
        }
    }
    
    class Bucket {
        private List<NodeStats> nodes = new ArrayList<NodeStats>();
        int nmax = Integer.MAX_VALUE;
        
        public boolean addNode(NodeStats node) {
            nodes.add(node);
            nmax = Math.min(node.degree + 1, nmax);
            return nmax <= nodes.size();
        }
    
        public List<NodeStats> getNodes() {
            return nodes;
        }
    
        public int getNmax() {
            return nmax;
        }
        
        public boolean merge(Bucket bucket) {
            nmax = Math.min(nmax,bucket.getNmax());
            nodes.addAll(bucket.getNodes());
            return nmax <= nodes.size();
        }
    }
    
    
    public long darwini(FileWriter writer,
                        Random random,
                        long offset) throws IOException {
    
        List<Bucket> filledBuckets = new ArrayList<Bucket>();
        HashMap<Integer, Bucket> currentBuckets = new HashMap<Integer,Bucket>();
        List<NodeStats>  nodes = new ArrayList<NodeStats>();
        for(int i = 0; i < modelId.length; ++i ) {
            Community model = streamer.getModel(modelId[i]);
            long baseOffset = offset + offsets.get(modelId[i]);
            for(int j = 0; j < numModels[i]; ++j) {
                for(int k = 0; k < model.getExternalTriangles().size(); ++k) {
                    int numTriangles = model.getExternalTriangles().get(k);
                    if(numTriangles > 0) {
                        int degree = model.getExcessDegree().get(k);
                        nodes.add(new NodeStats(k + baseOffset, numTriangles, degree));
                    }
                }
                baseOffset+=model.getSize();
            }
        }
        
        int index = 0;
        for(NodeStats node : nodes) {
            Bucket bucket = currentBuckets.get(node.numTriangles);
            if(bucket == null) {
                bucket = new Bucket();
                currentBuckets.put(node.numTriangles, bucket);
            }
            
            if(bucket.addNode(node)) {
                currentBuckets.remove(node.numTriangles);
                filledBuckets.add(bucket);
            }
    
            index++;
        }
        
        List<Bucket> toMergeBuckets = new ArrayList<Bucket>(currentBuckets.values());
        toMergeBuckets.sort(new Comparator<Bucket>() {
            @Override
            public int compare(Bucket bucket1, Bucket bucket2) {
                return bucket1.getNodes().size() - bucket2.getNodes().size();
            }
        });
        
        Bucket bucket = new Bucket();
        
        for(Bucket nextBucket : toMergeBuckets) {
            if(bucket.merge(nextBucket)) {
                filledBuckets.add(bucket);
                bucket = new Bucket();
            }
        }
        
        if(bucket.getNodes().size() > 0) {
            filledBuckets.add(bucket);
        }
        
        long numGeneratedEdges = 0L;
        for(Bucket nextBucket : filledBuckets) {
            long numTriangles = nextBucket.getNodes().get(0).numTriangles;
            long size = nextBucket.getNodes().size();
            if(size > 2) {
                double prob = Math.pow(2.0 * numTriangles / (double) ((size - 1) * (size - 2)), 1 / 3.0);
                for (int i = 0; i < size; i++) {
                    for (int j = i + 1; j < size; j++) {
                        if (random.nextDouble() < prob) {
                            long tail = nextBucket.getNodes().get(i).nodeId;
                            long head = nextBucket.getNodes().get(j).nodeId;
                            writer.write(tail + "\t" + head + "\n");
                            numGeneratedEdges++;
                        }
                    }
                }
            }
        }
        return numGeneratedEdges;
        
    }
    public long getNumCommunities() {
        return numCommunities;
    }
    
    public Map<Integer, Long> getCounts() {
        return counts;
    }
}
