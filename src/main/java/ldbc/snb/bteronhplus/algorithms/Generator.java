package ldbc.snb.bteronhplus.algorithms;

import ldbc.snb.bteronhplus.structures.*;
import org.jgrapht.Graph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.jgrapht.graph.builder.GraphBuilder;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

public class Generator {
    
    public static void sampleEdges(Random random,
                                   EdgeWriter writer,
                                   BlockModel blockModel,
                                   List<Map<Integer,Long>> partition,
                                   CommunityStreamer communityStreamer,
                                   int threadId,
                                   int numThreads) throws IOException {
    
        // Counting total number of nodes
        long totalDegree = 0L;
        long totalExternalDegree = 0L;
        long totalNumNodes = 0L;
        long internalDegreePerBlock[] = new long[partition.size()];
        long totalDegreePerBlock[] = new long[partition.size()];
        Arrays.fill(internalDegreePerBlock, 0L);
        Arrays.fill(totalDegreePerBlock, 0L);
    
        int index = 0;
        for(Map<Integer,Long> counts : partition) {
            for(Map.Entry<Integer,Long> entry : counts.entrySet()) {
                Community model = communityStreamer.getModel(entry.getKey());
                totalNumNodes += entry.getValue()*model.getSize();
                totalDegree += entry.getValue()*(model.getExternalDegree()+model.getInternalDegree());
                totalExternalDegree += entry.getValue()*(model.getExternalDegree());
                internalDegreePerBlock[index] += entry.getValue()*model.getInternalDegree();
                totalDegreePerBlock[index] += entry.getValue()*(model.getInternalDegree()+model.getExternalDegree());
            }
            index++;
        }
    
    
        // Computing offsets and generating internal community edges
        long offsets[] = new long[partition.size()];
    
        offsets[0] = 0;
        for(int i = 1; i < offsets.length; ++i) {
            Map<Integer,Long> counts = partition.get(i-1);
            long blockSize = 0;
            for(Map.Entry<Integer,Long> entry : counts.entrySet()) {
                Community model = communityStreamer.getModel(entry.getKey());
                blockSize += entry.getValue()*model.getSize();
            }
            offsets[i] = offsets[i-1] + blockSize;
        }
    
        List<BlockSampler> blockSamplers = new ArrayList<BlockSampler>();
    
        for(Map<Integer,Long> entry : partition ) {
            blockSamplers.add(new BlockSampler(entry, communityStreamer));
        }
    
    
        System.out.println("Generating external edges");
        System.out.println("Total number of external edges to generate: "+(totalExternalDegree/2));
        long totalExternalGeneratedEdges = 0;
        long totalExpectedGeneratedEdges = 0;
        double sumDensities = 0.0;
        long totalBrokenBlocks = 0L;
        long totalNumEdgesBelowThreshold = 0L;
        long totalNumEdgesNegative = 0L;
        long numCommunitiesIfWellConnected = 0L;
        long numExcess = 0;
        for(BlockModel.ModelEntry entry : blockModel.getEntries().values()) {
            for(Map.Entry<Integer,Double> neighbor : entry.degree.entrySet()) {
                sumDensities += neighbor.getValue();
                BlockSampler sampler = blockSamplers.get(entry.id);
                GraphBuilder builder = SimpleGraph.createBuilder(DefaultEdge.class);
            
                double expectedDegree = entry.totalDegree * totalDegree;
                if(entry.id == neighbor.getKey() && entry.id % numThreads == threadId) {
                
                    long numEdges = (Math.round(neighbor.getValue() * totalDegree) - internalDegreePerBlock[entry
                        .id])/2;
                
                
                    if(numEdges < (sampler.getNumCommunities()-1) && sampler.getNumCommunities() > 1){
                        totalNumEdgesBelowThreshold++;
                        if(numEdges < 0) totalNumEdgesNegative++;
                    } else {
                        numCommunitiesIfWellConnected++;
                    }
                
                    sampler.generateCommunityEdges(writer,
                                                   partition.get(entry.id),
                                                   communityStreamer,
                                                   offsets[entry.id],
                                                   builder);
                
                    long darwiniEdges = sampler.darwini(writer,random,offsets[entry.id]);
                    totalExternalGeneratedEdges+=darwiniEdges;
                    numEdges-=darwiniEdges;
                
                    sampler.generateConnectedGraph(writer, random, offsets[entry.id], builder);
                
                    numEdges -= sampler.getNumCommunities() - 1;
                
                    for(int i = 0; i < numEdges; ++i) {
                        long node1 = sampler.sample(random, offsets[entry.id]);
                        long node2 = sampler.sample(random, offsets[entry.id]);
                        totalExpectedGeneratedEdges++;
                        if (node1 != -1 && node2 != -1) {
                            writer.write(node1, node2);
                            totalExternalGeneratedEdges++;
                        
                            if(node1 != node2) {
                                builder.addEdge(node1, node2);
                            }
                        }
                    }
                
                    Graph<Long, DefaultEdge> graph = builder.build();
                    ConnectivityInspector<Long, DefaultEdge> connectivityInspector = new ConnectivityInspector<>(graph);
                    List<Set<Long>> connectedComponents = connectivityInspector.connectedSets();
                    if (connectedComponents.size() > 1) {
                        totalBrokenBlocks++;
                    }
                }
            }
        }
    
        for(BlockModel.ModelEntry entry : blockModel.getEntries().values()) {
            for (Map.Entry<Integer, Double> neighbor : entry.degree.entrySet()) {
            
            
                if(entry.id < neighbor.getKey()) {
                    BlockSampler sampler1 = blockSamplers.get(entry.id);
                    BlockSampler sampler2 = blockSamplers.get(neighbor.getKey());
                    long numEdges= Math.round(neighbor.getValue() * totalDegree);
                    
                    long numEdgesPerThread = numEdges / numThreads;
                    if(entry.id % numThreads == threadId) {
                        numEdgesPerThread += numEdges % numThreads;
                    }
                    
                    for(int i = 0; i < numEdgesPerThread; ++i) {
                        long node1 = sampler1.sample(random, offsets[entry.id]);
                        long node2 = sampler2.sample(random, offsets[neighbor.getKey()]);
                        totalExpectedGeneratedEdges++;
                        if (node1 != -1 && node2 != -1) {
                            writer.write(node1,node2);
                            totalExternalGeneratedEdges++;
                        }
                    }
                
                }
            
            }
        }
    
    
        System.out.println("Total number of external edges generated: "+totalExternalGeneratedEdges);
        System.out.println("Total expected generated: "+totalExpectedGeneratedEdges);
        System.out.println("Total sum densities: "+sumDensities);
        System.out.println("Total broken blocks: "+totalBrokenBlocks);
        System.out.println("Total num edges below threshold: "+totalNumEdgesBelowThreshold);
        System.out.println("Total num edges negative: "+totalNumEdgesNegative);
        System.out.println("Total num communities if well connected: "+numCommunitiesIfWellConnected);
    
    }
    
}
