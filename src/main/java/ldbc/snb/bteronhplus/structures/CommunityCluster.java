package ldbc.snb.bteronhplus.structures;

import org.apache.commons.math3.util.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class CommunityCluster implements SuperNode {

    private int             id;
    private List<SuperNode> children;
    private long            offsets[] = null;
    private int             size = 0;
    private long            internalDegree = 0;
    private long            externalDegree = 0;
    private double          sampleNodeExternalCumProbability[];
    private double          sampleNodeInternalProbability[];
    private double          sampleInterNodeProbability[];
    private long            interDegree[];
    private int             sampleNodeExternalIndices[];
    private double          intraNodeEdgeProb;
    
    private double          expectedRatio = 0.0;

    public CommunityCluster(int id,
                            List<Community> children,
                            double externalRatio
                            ) {
        this.id = id;
        this.expectedRatio = externalRatio;
        if(id == 0) {
            System.out.println();
        }
        this.children = new ArrayList<SuperNode>(children);
        this.children.sort( new Comparator<SuperNode>() {
            @Override
            public int compare(SuperNode superNode1, SuperNode superNode2) {
                return -1*(int)(superNode1.getExternalDegree()-superNode2.getExternalDegree());
            }
        });
        
        this.offsets = new long[this.children.size()];
    
        this.sampleNodeExternalIndices = new int[this.children.size()];

        offsets[0] = 0;
        for(int i = 1; i < offsets.length; ++i) {
            offsets[i] = offsets[i-1] + this.children.get(i-1).getSize();
        }

        long tempInternalZeroExternalDegree = 0;
        long tempInternalDegree = 0;
        long tempExternalDegree = 0;
        for(int i = 0; i < this.children.size(); ++i) {
            SuperNode node = this.children.get(i);
            tempInternalDegree += node.getInternalDegree();
            tempExternalDegree += node.getExternalDegree();
            if(node.getExternalDegree() == 0) {
                tempInternalZeroExternalDegree += node.getInternalDegree();
            }
            size+=node.getSize();
        }
        
        if(id == 19) {
            System.out.println();
        }
        long totalDegree = tempInternalDegree+tempExternalDegree;
        double finalRatio = Math.min((totalDegree - tempInternalDegree )/ (double)(totalDegree), externalRatio);
        boolean special = false;
        if(externalRatio == 0.0 && tempExternalDegree > 0) {
            finalRatio = (totalDegree - tempInternalDegree )/ (double)(totalDegree);
            special = true;
        }
        long externalEdges[] = new long[this.children.size()];
        Arrays.fill(externalEdges, 0);
        
        long externalDegreeBudget = (long)(totalDegree*finalRatio);
        /*double zeroExternalRatio = tempInternalZeroExternalDegree / (double)tempInternalDegree;
        externalDegreeBudget -= externalDegreeBudget*zeroExternalRatio;*/
        long totalToExternal = 0;
        
        for(int i = 0; i < this.children.size() && externalDegreeBudget > 0; i++) {
            SuperNode child = this.children.get(i);
            long toRemove = 0;
            if(!special) {
                toRemove = Math.min((long)(child.getExternalDegree()*0.5),externalDegreeBudget);
                //toRemove = Math.min(toRemove, externalDegreeBudget / 3);
                
            } else {
                toRemove = Math.min((long)(child.getExternalDegree()),externalDegreeBudget);
            }
            externalEdges[i] = toRemove;
            externalDegreeBudget-=toRemove;
            totalToExternal+=toRemove;
        }
        externalDegree=totalToExternal;
    
        internalDegree = totalDegree-externalDegree;
        long intraDegree = tempInternalDegree;
        intraNodeEdgeProb = intraDegree / (double)(internalDegree);
    
        int numExternalNonZero = 0;
        for(int i = 0; i < this.children.size(); ++i) {
            if(externalEdges[i] > 0) {
                sampleNodeExternalIndices[numExternalNonZero] = i;
                numExternalNonZero++;
            }
        }
    
        sampleNodeExternalCumProbability = new double[numExternalNonZero];
        sampleNodeInternalProbability = new double[this.children.size()];
        sampleInterNodeProbability = new double[this.children.size()];
        interDegree = new long[this.children.size()];

        if(numExternalNonZero > 0) {
            sampleNodeExternalCumProbability[0] = 0.0;
            for (int i = 1; i < numExternalNonZero; ++i) {
                sampleNodeExternalCumProbability[i] = sampleNodeExternalCumProbability[i - 1] +
                    externalEdges[sampleNodeExternalIndices[i - 1]] /
                        (double) externalDegree;
            }
        }
    
        for (int i = 0; i < sampleInterNodeProbability.length; ++i) {
            SuperNode child = this.children.get(i);
            long childInternalDegree = child.getExternalDegree() - externalEdges[i];
            interDegree[i] = childInternalDegree;
            sampleInterNodeProbability[i] = (internalDegree - intraDegree) > 0 ? childInternalDegree /
                (double) (internalDegree - intraDegree) : 0.0;
        }
    
        for (int i = 0; i < sampleNodeInternalProbability.length; ++i) {
            SuperNode child = this.children.get(i);
            long childInternalDegree = child.getInternalDegree();
            sampleNodeInternalProbability[i] = childInternalDegree / (double) tempInternalDegree;
        }
        
        
        long childrenDegree = tempInternalDegree+tempExternalDegree;
        long currentDegree = internalDegree + externalDegree;
        
        if(childrenDegree != currentDegree) {
            throw new RuntimeException("Found inconcistency between children and current degree "+currentDegree+" " +
                                           ""+childrenDegree);
        }
        
    }


    @Override
    public long getSize() {
        return size;
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
    public int getId() {
        return id;
    }

    @Override
    public void sampleEdges(FileWriter writer, Random random, long numEdges, long offset) throws IOException {
        
        // Creating intra node edges
        long intraNodeEdges = Math.round(intraNodeEdgeProb * numEdges);
        for(int i = 0; i < this.children.size(); ++i) {
            SuperNode child = this.children.get(i);
            long numEdgesToCreate = (Math.round(intraNodeEdges * sampleNodeInternalProbability[i]));
            child.sampleEdges(writer, random, numEdgesToCreate, offset + offsets[i]);
        }
        
    
        ArrayList<Pair<Integer,Long>> interDegrees = new ArrayList<Pair<Integer,Long>>();
        for(int i = 0; i < interDegree.length; ++i) {
            interDegrees.add(new Pair<Integer,Long>(i, interDegree[i]));
        }
        
        interDegrees.sort(new Comparator<Pair<Integer,Long>>() {
    
            @Override
            public int compare(Pair<Integer, Long> pair1, Pair<Integer, Long> pair2) {
                if( pair1.getValue() > pair2.getValue()) return -1;
                if( pair1.getValue() == pair2.getValue()) return 0;
                return 1;
            }
            
        });
    
    
        long degreeBudget[] = new long[interDegrees.size()];
        int indices[] = new int[interDegrees.size()];
        
        for(int i = 0; i  < interDegrees.size(); ++i) {
            degreeBudget[i] = interDegrees.get(i).getValue();
            indices[i] = interDegrees.get(i).getKey();
        }
        if(id == 19) {
            System.out.println();
        }
        
        boolean finished = false;
        while(!finished) {
            finished = true;
            for (int i = 0; i < degreeBudget.length; ++i) {
                for (int j = i + 1; j < degreeBudget.length && degreeBudget[i] > 0; ++j) {
                    if (degreeBudget[j] > 0) {
                        int index1 = indices[i];
                        int index2 = indices[j];
                        long node1 = children.get(index1).sampleNode(random, offset + offsets[index1]);
                        long node2 = children.get(index2).sampleNode(random, offset + offsets[index2]);
                        writer.write(node1 + "\t" + node2 + "\n");
                        degreeBudget[i]--;
                        degreeBudget[j]--;
                        finished = false;
                    }
                }
            }
        }
        
    
        int indicesNonZero[] = new int[indices.length];
        int numNonZeros = 0;
        long remainingDegree = 0;
        for(int i = 0; i < degreeBudget.length; ++i)  {
            remainingDegree+=degreeBudget[i];
            if(degreeBudget[i] != 0L) {
                indicesNonZero[numNonZeros] = indices[i];
                numNonZeros++;
            }
        }
        
        if(numNonZeros > 0) {
            double cumulative[] = new double[numNonZeros];
            cumulative[0] = 0.0;
            for (int i = 1; i < numNonZeros; ++i) {
                double currentProb = degreeBudget[indicesNonZero[i - 1]] / (double) remainingDegree;
                cumulative[i] = currentProb + cumulative[i - 1];
            }
    
    
            long numEdgesRemaining = remainingDegree / 2;
            for (int i = 0; i < numEdgesRemaining; ++i) {
        
                int pos = Arrays.binarySearch(cumulative, random.nextDouble());
        
                if (pos < 0) {
                    pos = -(pos + 1);
                    pos--;
                }
        
                int first = indicesNonZero[pos];
        
                pos = Arrays.binarySearch(cumulative, random.nextDouble());
        
                if (pos < 0) {
                    pos = -(pos + 1);
                    pos--;
                }
        
                int second = indicesNonZero[pos];
        
                long node1 = children.get(first).sampleNode(random, offset + offsets[first]);
                long node2 = children.get(second).sampleNode(random, offset + offsets[second]);
                writer.write(node1 + "\t" + node2 + "\n");
            }
        }
        
    }

    public List<SuperNode> getChildren() {
        return children;
    }

    @Override
    public long sampleNode(Random random, long offset) {

        int pos = Arrays.binarySearch(sampleNodeExternalCumProbability,random.nextDouble());

        if(pos < 0) {
            pos  = -(pos+1);
            pos--;
        }

        if(pos >= children.size()) {
            pos = children.size() - 1;
        }
        
        if(pos == -1) {
           pos = 0;
        }
        
        int index = sampleNodeExternalIndices[pos];
        return children.get(index).sampleNode(random, offset + offsets[index]);
    }
    
}
