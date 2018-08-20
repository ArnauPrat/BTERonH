package ldbc.snb.bteronhplus.algorithms;

import ldbc.snb.bteronhplus.structures.BlockModel;
import ldbc.snb.bteronhplus.structures.SuperNode;
import ldbc.snb.bteronhplus.structures.CommunityStreamer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Partitioning {
    
    
    private static double combineScores(double sizeScore,
                                        double internalDegreeScore,
                                        double externalDegreeScore) {
        return (/*sizeScore + */internalDegreeScore + externalDegreeScore) ;
    }
    
    private static double computeScore(long totalSize,
                                       long totalDegree,
                                       BlockModel.ModelEntry entry,
                                       double size,
                                       double internalDegree,
                                       double externalDegree) {
        
        double expectedSize = (entry.size*totalSize);
        double expectedInternalDegree = (entry.totalDegree - entry.externalDegree)*totalDegree;
        double expectedExternalDegree = (entry.externalDegree)*totalDegree;
    
        double sizeScore = Math.pow((expectedSize-size),2);
        double internalDegreeScore = Math.pow((expectedInternalDegree-internalDegree), 2);
        double externalDegreeScore = Math.pow((expectedExternalDegree-externalDegree),2);
        return combineScores(sizeScore, internalDegreeScore, externalDegreeScore);
        
    }
    
    private static double initializePartialScores(BlockModel blockModel,
                                double partialScores[],
                                long totalSize,
                                long totalDegree,
                                long currentBlockSize[],
                                long currentBlockInternalDegree[],
                                long currentBlockExternalDegree[]
                                ) {
    
        int numBlocks = blockModel.getNumBlocks();
        double score = 0.0;
        for(int i = 0; i < numBlocks; ++i) {
            BlockModel.ModelEntry entry = blockModel.getEntries().get(i);
            double blockSize = currentBlockSize[i];
            double blockTotalDegree = currentBlockInternalDegree[i] + currentBlockExternalDegree[i];
            double blockRatio = entry.externalDegree / entry.totalDegree;
            double blockInternalDegree = (blockTotalDegree*(1.0-blockRatio));
            double blockExternalDegree = (blockTotalDegree*blockRatio);
            
            partialScores[i] = computeScore(totalSize,
                                            totalDegree,
                                            entry,
                                            blockSize,
                                            blockInternalDegree,
                                            blockExternalDegree);
            score += partialScores[i];
        }
        return score;
    }
    

    private static double score(BlockModel blockModel,
                                double currentScore,
                                long totalSize,
                                long totalDegree,
                                long currentBlockSize[],
                                long currentBlockInternalDegree[],
                                long currentBlockExternalDegree[],
                                long size,
                                long internalDegree,
                                long externalDegree,
                                int block) {
        

        double score = currentScore;
    
        BlockModel.ModelEntry entry = blockModel.getEntries().get(block);
        double blockSize = currentBlockSize[block];
        double expectedRatio = entry.externalDegree / entry.totalDegree;
        blockSize+=size;
        double blockTotalDegree = currentBlockInternalDegree[block] +
                                  currentBlockExternalDegree[block] +
                                  internalDegree +
                                  externalDegree;
        double blockRatio = expectedRatio;
    
        double blockInternalDegree = (blockTotalDegree*(1.0-blockRatio));
        double blockExternalDegree = (blockTotalDegree*(blockRatio));
    
    
        score += computeScore(totalSize,
                              totalDegree,
                              entry,
                              blockSize,
                              blockInternalDegree,
                              blockExternalDegree);
        return score;
    }
    
    
    public static List<Map<Integer,Long>> partition(Random random, BlockModel blockModel,
                                                    CommunityStreamer streamer,
                                                    long targetNumNodes,
                                                    long threadNumNodes,
                                                    int  threadId,
                                                    int  numThreads) {

        List<Map<Integer,Long>> superNodesPerBlock = new ArrayList<Map<Integer,Long>>();
        int numBlocks = blockModel.getNumBlocks();
        for(int i = 0; i < numBlocks; ++i) {
            superNodesPerBlock.add(new HashMap<Integer,Long>());
        }

        long currentBlockSize[] = new long[numBlocks];
        Arrays.fill(currentBlockSize, 0L);
        long currentBlockInternalDegree[] = new long[numBlocks];
        Arrays.fill(currentBlockInternalDegree, 0L);
        long currentBlockExternalDegree[] = new long[numBlocks];
        Arrays.fill(currentBlockExternalDegree, 0L);
        
        double partialScores[] = new double[numBlocks];
        Arrays.fill(partialScores,0.0);
    
        long totalNodes = 0L;
        long totalDegree = 0L;
    
        List<SuperNode> superNodes = new ArrayList<SuperNode>();
        while( totalNodes < threadNumNodes) {
            SuperNode nextSuperNode = streamer.next(random);
            superNodes.add(nextSuperNode);
            totalNodes+=nextSuperNode.getSize();
            totalDegree+=nextSuperNode.getInternalDegree();
            totalDegree+=nextSuperNode.getExternalDegree();
    
    
            /*System.out.println("Distributed "+totalNodes+" out of " +
                                   threadNumNodes);*/
        }
        
        double factor = targetNumNodes / (double)threadNumNodes;
        totalDegree *= factor;
    
        for(int i = 0; i < numBlocks; ++i) {
            BlockModel.ModelEntry entry = blockModel.getEntries().get(i);
            if(entry.size * targetNumNodes < 100) {
                if (i % numThreads != threadId) {
                    currentBlockSize[i] = (long) (entry.size * targetNumNodes);
                    long currentTotalDegree = (long)(entry.totalDegree * totalDegree);
                    currentBlockInternalDegree[i] = (long) (currentTotalDegree*(1.0-entry.externalDegree));
                    currentBlockExternalDegree[i] = (long) (currentTotalDegree*(entry.externalDegree));
                }
            } else {
                currentBlockSize[i] = (numThreads-1)*(long) (entry.size * targetNumNodes) / numThreads;
                long currentTotalDegree = (numThreads-1)*(long)(entry.totalDegree * totalDegree) / numThreads;
                currentBlockInternalDegree[i] = (long) (currentTotalDegree*(1.0-entry.externalDegree));
                currentBlockExternalDegree[i] = (long) (currentTotalDegree*(entry.externalDegree));
            }
        }
    
        superNodes.sort(new Comparator<SuperNode>() {
            @Override
            public int compare(SuperNode superNode1, SuperNode superNode2) {
                if(superNode1.getSize() > superNode2.getSize()) return -1;
                if(superNode1.getSize() == superNode2.getSize()) return 0;
                return 1;
            }
        });
        
        int count = 1;
    
        long totalObservedNodes = 0L;
        long totalObservedDegree = 0L;
        
        for( SuperNode nextSuperNode : superNodes) {
    
            /*int bestBlock = random.nextInt(blockModel.getNumBlocks() / numThreads);
            bestBlock = threadId + (bestBlock * numThreads);*/
            int bestBlock = threadId;
            
            double currentScore = initializePartialScores(blockModel,
                                     partialScores,
                                     totalNodes,
                                     totalDegree,
                                     currentBlockSize,
                                     currentBlockInternalDegree,
                                     currentBlockExternalDegree);
    
            double nextScore = score(blockModel,
                                     currentScore - partialScores[bestBlock],
                                     totalNodes,
                                     totalDegree,
                                     currentBlockSize,
                                     currentBlockInternalDegree,
                                     currentBlockExternalDegree,
                                     nextSuperNode.getSize(),
                                     nextSuperNode.getInternalDegree(),
                                     nextSuperNode.getExternalDegree(),
                                     bestBlock);
            
            double bestImprovement = currentScore - nextScore;
            
            for(int j = 0; j < numBlocks; ++j) {
    
    
                BlockModel.ModelEntry entry = blockModel.getEntries().get(j);
    
                if(entry.id % numThreads == threadId || entry.size*targetNumNodes >= 100 ) {
    
                    nextScore = score(blockModel,
                                      currentScore - partialScores[j],
                                      totalNodes,
                                      totalDegree,
                                      currentBlockSize,
                                      currentBlockInternalDegree,
                                      currentBlockExternalDegree,
                                      nextSuperNode.getSize(),
                                      nextSuperNode.getInternalDegree(),
                                      nextSuperNode.getExternalDegree(),
                                      j);
    
    
                    double improvement = currentScore - nextScore;
    
                    if (improvement > bestImprovement) {
                        bestBlock = j;
                        bestImprovement = improvement;
                    }
                }
            }
    
            BlockModel.ModelEntry entry = blockModel.getEntries().get(bestBlock);
            double expectedRatio = entry.externalDegree / entry.totalDegree;
            currentBlockSize[bestBlock]+=nextSuperNode.getSize();
            long currentBlockDegree =   currentBlockInternalDegree[bestBlock] +
                                        currentBlockExternalDegree[bestBlock] +
                                        nextSuperNode.getInternalDegree()+
                                        nextSuperNode.getExternalDegree();
    
            currentBlockInternalDegree[bestBlock] = (long)(currentBlockDegree*(1-expectedRatio));
            currentBlockExternalDegree[bestBlock] = currentBlockDegree -
                                                    currentBlockInternalDegree[bestBlock];
    
            superNodesPerBlock.get(bestBlock).merge(nextSuperNode.getId(), 1L, Long::sum);
    
            totalObservedDegree+=nextSuperNode.getInternalDegree()+nextSuperNode.getExternalDegree();
            totalObservedNodes+=nextSuperNode.getSize();
            
            if(totalObservedNodes % 10000 == 0) {
                System.out.println("Distributed "+totalObservedNodes+" out of "+threadNumNodes);
            }

            count++;
        }
        
        System.out.println("Number of nodes observed "+totalObservedNodes);
        System.out.println("Number of degree observed "+totalObservedDegree);
        System.out.println("Number of super nodes consumed "+count);
        return superNodesPerBlock;
    }
    
    public static void printStats(BlockModel blockModel, List<Map<Integer,Long>> partition, CommunityStreamer
        streamer) {
        
        long currentBlockSize[] = new long[partition.size()];
        long currentBlockInternalDegree[] = new long[partition.size()];
        long currentBlockExternalDegree[] = new long[partition.size()];
        double currentBlockRatio[] = new double[partition.size()];
        
        for(int i = 0; i < partition.size(); ++i) {
    
            BlockModel.ModelEntry entry = blockModel.getEntries().get(i);
            double expectedRatio = entry.externalDegree / entry.totalDegree;
            for(Map.Entry<Integer,Long> count : partition.get(i).entrySet()) {
                SuperNode nextSuperNode = streamer.getModel(count.getKey());
                long numModels = count.getValue();
                currentBlockSize[i] += nextSuperNode.getSize()*numModels;
                long currentBlockDegree = currentBlockInternalDegree[i] +
                    currentBlockExternalDegree[i] +
                    (nextSuperNode.getInternalDegree() +
                    nextSuperNode.getExternalDegree())*numModels;
        
                currentBlockRatio[i] = expectedRatio;
        
                currentBlockInternalDegree[i] = (long) (currentBlockDegree * (1 - currentBlockRatio[i]));
                currentBlockExternalDegree[i] = currentBlockDegree - currentBlockInternalDegree[i];
            }
            
        }
        
        
        long totalDegree = 0;
        for(int i = 0; i < currentBlockInternalDegree.length; ++i) {
            totalDegree+=currentBlockInternalDegree[i];
            totalDegree+=currentBlockExternalDegree[i];
        }
        
        long totalSize = 0;
        for(int i = 0; i < currentBlockSize.length; ++i) {
            totalSize += currentBlockSize[i];
        }
        
        long totalExpectedInternalDegree = 0;
        long totalExpectedExternalDegree = 0;
        
        long totalObservedInternalDegree = 0;
        long totalObservedExternalDegree = 0;
        
        double sizeScore = 0.0;
        double internalDegreeScore = 0.0;
        double externalDegreeScore = 0.0;
        
        try {
            FileWriter partitioningSizes = new FileWriter(new File("partitioning.sizes"));
            for(BlockModel.ModelEntry entry : blockModel.getEntries().values()) {
                long expectedSize = (long)(entry.size*totalSize);
                long observedSize = (long)(currentBlockSize[(int)entry.id]);
                partitioningSizes.write(entry.id+" "+expectedSize+" "+observedSize+"\n");
                sizeScore += Math.pow((expectedSize-observedSize), 2 );
            }
            partitioningSizes.close();
            
            FileWriter partitioningInternalDegree = new FileWriter(new File("partitioning.internalDegree"));
            for(BlockModel.ModelEntry entry : blockModel.getEntries().values()) {
                long expectedInternalDegree = (long)((entry.totalDegree - entry.externalDegree)*totalDegree);
                totalExpectedInternalDegree += expectedInternalDegree;
                long observedInternalDegree = (long)(currentBlockInternalDegree[(int)entry.id]);
                totalObservedInternalDegree += observedInternalDegree;
                
                internalDegreeScore += Math.pow((expectedInternalDegree - observedInternalDegree) ,2);
                
                partitioningInternalDegree.write(entry.id+" "+expectedInternalDegree+" " +
                                                     ""+observedInternalDegree+"\n");
            }
            partitioningInternalDegree.close();
            
            FileWriter partitioningExternalDegree = new FileWriter(new File("partitioning.externalDegree"));
            for(BlockModel.ModelEntry entry : blockModel.getEntries().values()) {
                long expectedExternalDegree = (long)(entry.externalDegree*totalDegree);
                totalExpectedExternalDegree += expectedExternalDegree;
                long observedExternalDegree = (long)(currentBlockExternalDegree[(int)entry.id]);
                
                totalObservedExternalDegree += observedExternalDegree;
                
                externalDegreeScore += Math.pow((expectedExternalDegree - observedExternalDegree),2);
                
                partitioningExternalDegree.write(entry.id+" "+expectedExternalDegree+" " +
                                                     observedExternalDegree+"\n");
            }
            partitioningExternalDegree.close();
        } catch( IOException e) {
            e.printStackTrace();
        }
        
        System.out.println("Expected vs Observed internal degree: "+totalExpectedInternalDegree+" "+totalObservedInternalDegree);
        System.out.println("Expected vs Observed external degree: "+totalExpectedExternalDegree+" " +
                               ""+totalObservedExternalDegree);
        
        System.out.println("Size Score: "+(long)sizeScore);
        System.out.println("Internal Degree Score: "+(long)internalDegreeScore);
        System.out.println("External Degree Score: "+(long)externalDegreeScore);
        
    }

}
