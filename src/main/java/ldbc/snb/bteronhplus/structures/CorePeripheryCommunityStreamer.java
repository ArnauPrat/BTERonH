package ldbc.snb.bteronhplus.structures;


import java.util.*;

/*

public class CorePeripheryCommunityStreamer implements CommunityStreamer {

    protected static int NUM_MODELS_PER_SIZE = 100;

    private Map<Integer, List<CommunityModel>>  communityModels = null;
    private List<Community>                     communities = null;
    private GraphStats                          graphStats = null;
    private Random                              random = null;
    private int                                 nextCommunityId = 0;
    
    private static class NodeInfo {
        public int   degree;
        public int  numTriangles;
        public double   clusteringCoefficient;
    }

        
    public static class CommunityModel {
        
        public int            size                  = 0;
        public List<NodeInfo> nodeInfo              = new ArrayList<NodeInfo>();
        public int            minDegree             = Integer.MAX_VALUE;
        public int            minTriangles          = Integer.MAX_VALUE;
        public double         coreDensity           = 1.0;
        public int            coreSize              = 0;
        public double         peripheryDensity[]    = null;
        public int            peripheryCoreThreshold[] = null;
        public double         triangleBudget[]      = null;
        public double         degreeBudget[]        = null;
        public int            currentCoreThreshold  = 0;
        public int            peripherySize         = 0;
        
        public CommunityModel(int size) {
            this.size = size;
            this.peripheryDensity = new double[size];
            this.triangleBudget = new double[size];
            this.degreeBudget = new double[size];
            this.peripheryCoreThreshold = new int[size];
        }
    
        public Community generate(int id) {
         
            Random random = new Random(id);
            List<Edge> edges = new ArrayList<Edge>();
            int numNodes = nodeInfo.size();
            for(int i = 0; i < coreSize; ++i) {
                for(int j = i+1; j < coreSize; ++j) {
                    double prob = random.nextDouble();
                    if(prob < coreDensity) {
                        edges.add(new Edge(i,j));
                    }
                }
            }
            
            for(int i = 0; i < peripherySize; ++i) {
                int peripheryIndex = coreSize+i;
                for(int j = 0; j < peripheryCoreThreshold[peripheryIndex]; ++j) {
                    double prob = random.nextDouble();
                    if(prob < peripheryDensity[peripheryIndex]) {
                        edges.add(new Edge(peripheryIndex,j));
                    }
                }
            }
            
            Map<Integer, Integer > degree = new HashMap<Integer, Integer>();
            for(Edge edge : edges) {
                degree.merge((int)edge.getTail(), 1 , Integer::sum);
                degree.merge((int)edge.getHead(), 1 , Integer::sum);
            }
            
            int excessDegree[] = new int[numNodes];
            for(Map.Entry<Integer,Integer> entry : degree.entrySet()) {
                excessDegree[entry.getKey()] = (int) Math.max(0.0, nodeInfo.get(entry.getKey()).degree - entry
                    .getValue());
            }
            
            return new Community(id, nodeInfo.size(), edges, excessDegree);
        }
        
    }
    
    private static int findBucket(List<CommunityModel> buckets, NodeInfo nodeInfo, int start ) {
        
        double bestScore = 0;
        int    bestBucket = -1;
        for(int i = 0; i < buckets.size(); ++i) {
            int index = (start + i) % buckets.size();
            CommunityModel nextBucket = buckets.get(index);
            if(nextBucket.nodeInfo.size() < nextBucket.size) {
                int coreSize = nextBucket.coreSize;
                int expectedTrianglesPerNode = (int)((nextBucket.coreSize-1)*(nextBucket.coreSize-2)*Math.pow(nextBucket
                                                                                                                  .coreDensity,3));
                int expectedTrianglesPerNodeAfterInsert = (int)((nextBucket.coreSize)*(nextBucket.coreSize-1)*Math.pow
                    (nextBucket.coreDensity,3)) / 2;
    
                if(coreSize < nodeInfo.degree &&
                   coreSize+1 < nextBucket.minDegree &&
                   expectedTrianglesPerNode <= nodeInfo.numTriangles &&
                   expectedTrianglesPerNodeAfterInsert <= nextBucket.minTriangles) {
                    if(bestBucket == -1) {
                        bestBucket = index;
                        return bestBucket;
                    }
                }
            }
        }
    
        return bestBucket;
    }
    
    private static void addToBucket(CommunityModel bucket, NodeInfo nodeInfo ) {
    
        bucket.nodeInfo.add(nodeInfo);
        bucket.minDegree = Math.min(nodeInfo.degree, bucket.minDegree);
        bucket.minTriangles = Math.min(nodeInfo.numTriangles, bucket.minTriangles);
        bucket.coreSize = bucket.nodeInfo.size();
    }
    
    private static boolean accommodateToCore(List<CommunityModel> buckets, NodeInfo nodeInfo) {
    
    
        CommunityModel bestBucket = null;
        double bestScore = 0.0;
        double bestAlpha = 0.0;
        boolean found = false;
        for(CommunityModel bucket : buckets) {
            if(bucket.nodeInfo.size() < bucket.size) {
                int coreSize = bucket.coreSize;
                double density = bucket.coreDensity;
                double minDegree = Math.min(bucket.minDegree, nodeInfo.degree);
                double minTriangles = Math.min(bucket.minTriangles, nodeInfo.numTriangles);
                double minAlpha1 = (coreSize*density - minDegree)/(double)coreSize;
                double minAlpha2 = density - Math.pow(2*minTriangles/((double)coreSize*(coreSize-1)), 1/3.0);
                double minAlpha = Math.max(Math.max(minAlpha1, minAlpha2), 0.0);
                double maxAlpha = density*(1 - Math.pow((coreSize-2)/(double)(coreSize+1), 1/3.0));
                if(maxAlpha > minAlpha) {
                    if(!found) {
                        bestBucket = bucket;
                        bestAlpha = minAlpha;
                        bestScore = (coreSize*(coreSize-1))*((coreSize+1)*Math.pow((density-minAlpha), 3.0) -
                            (coreSize-2)*Math.pow(density,3.0));
                    }
                    found = true;
                    double newScore = (coreSize*(coreSize-1))*((coreSize+1)*Math.pow((density-minAlpha), 3.0) -
                            (coreSize-2)*Math.pow(density,3.0));
                    if(bestScore < newScore) {
                        bestBucket = bucket;
                        bestAlpha = minAlpha;
                        bestScore = newScore;
                    }
                    
                }
            }
        }
        
        
        if(found) {
            bestBucket.nodeInfo.add(nodeInfo);
            bestBucket.coreSize++;
            bestBucket.minDegree = Math.min(nodeInfo.degree, bestBucket.minDegree);
            bestBucket.minTriangles = Math.min(nodeInfo.numTriangles, bestBucket.minTriangles);
            bestBucket.coreDensity -= bestAlpha;
            return true;
        } else {
    
            for(CommunityModel bucket : buckets) {
                if(bucket.coreSize <=2 && bucket.nodeInfo.size() < bucket.size) {
                    bucket.nodeInfo.add(nodeInfo);
                    bucket.coreSize++;
                    bucket.minDegree = Math.min(nodeInfo.degree, bucket.minDegree);
                    bucket.minTriangles = Math.min(nodeInfo.numTriangles, bucket.minTriangles);
                    bucket.coreDensity -= bestAlpha;
                    return true;
                }
            }
        }
        return false;
    }
    
    private static boolean addToPeriphery(List<CommunityModel> buckets, NodeInfo nodeInfo) {
    
        for(CommunityModel bucket : buckets) {
            if(bucket.nodeInfo.size() < bucket.size) {
                if(nodeInfo.degree >= 2) {
                    int subCoreSize = bucket.currentCoreThreshold;
                    double density = bucket.coreDensity;
                    int numTriangles = nodeInfo.numTriangles;
                    int degree = Math.min(subCoreSize, nodeInfo.degree);
                    double requiredDensity = Math.sqrt(2 * numTriangles / (double) (density * degree * (degree - 1)));
                    if (requiredDensity <= 1.0) {
                        double numTrianglesClosedPerNode = (degree * (degree - 1) * Math.pow(requiredDensity, 2.0) * density /
                            (2.0 * degree));
                        double degreeRealizedPerNode = requiredDensity;
                        boolean realizable = true;
                        for(int i = 0; i < degree; ++i) {
                            if (bucket.triangleBudget[i] - numTrianglesClosedPerNode < 0 ||
                                bucket.degreeBudget[i] - degreeRealizedPerNode < 0) {
                                realizable = false;
                                break;
                            }
                        }
                        
                        if(realizable) {
                            int peripheryIndex = bucket.coreSize + bucket.peripherySize;
                            bucket.peripheryDensity[peripheryIndex] = requiredDensity;
                            bucket.peripheryCoreThreshold[peripheryIndex] = degree;
                            bucket.nodeInfo.add(nodeInfo);
                            bucket.peripherySize++;
    
                            for (int i = bucket.currentCoreThreshold - 1; i >= 0; --i) {
                                bucket.triangleBudget[i] -= numTrianglesClosedPerNode;
                                bucket.degreeBudget[i] -= degreeRealizedPerNode;
                                if (bucket.triangleBudget[i] <= 0 ||
                                    bucket.degreeBudget[i] <= 0) {
                                    bucket.currentCoreThreshold--;
                                }
                            }
    
                            return true;
                        }
                    }
                } else {
                    double degreeRealizedPerNode = 1/(double)bucket.currentCoreThreshold;
                    int coreThresholdIndex = bucket.currentCoreThreshold - 1;
                    int numTrianglesClosedPerNode = 0;
                    if (coreThresholdIndex >= 0 && bucket.triangleBudget[coreThresholdIndex] -
                        numTrianglesClosedPerNode > 0 &&
                        bucket.degreeBudget[coreThresholdIndex] - degreeRealizedPerNode > 0) {
        
                        int peripheryIndex = bucket.coreSize + bucket.peripherySize;
                        bucket.peripheryDensity[peripheryIndex] = 1.0;
                        bucket.peripheryCoreThreshold[peripheryIndex] = bucket.currentCoreThreshold;
                        bucket.nodeInfo.add(nodeInfo);
                        bucket.peripherySize++;
                        for (int i = bucket.currentCoreThreshold - 1; i >= 0; --i) {
                            bucket.triangleBudget[i] -= numTrianglesClosedPerNode;
                            bucket.degreeBudget[i] -= degreeRealizedPerNode;
                            if (bucket.triangleBudget[i] <= 1 ||
                                bucket.degreeBudget[i] <= 1) {
                                bucket.currentCoreThreshold--;
                            }
                        }
                        return true;
                    }
                }
            }
        }
        
        return false;
    }
    
    
    
    public static List<CommunityModel> createModels(int modelSize,
                                                     int numModels,
                                                     List<Integer>  degree,
                                                     List<Double>   clusteringCoefficient) {
        
        List<CommunityModel> buckets = new ArrayList<CommunityModel>();
        for(int i = 0; i < numModels; ++i) {
            buckets.add(new CommunityModel(modelSize));
        }
        
        ArrayList<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();
        for(int i = 0; i < degree.size(); ++i) {
            NodeInfo nodeInfo = new NodeInfo();
            nodeInfo.degree = degree.get(i);
            nodeInfo.numTriangles = (int)(nodeInfo.degree*(nodeInfo.degree-1)*clusteringCoefficient.get(i)/2.0);
            nodeInfo.clusteringCoefficient = clusteringCoefficient.get(i);
            nodeInfos.add(nodeInfo);
        }
        
        nodeInfos.sort( new Comparator<NodeInfo>() {
            @Override
            public int compare(NodeInfo node1, NodeInfo node2) {
                if(node1.numTriangles != node2.numTriangles) {
                    return node1.numTriangles - node2.numTriangles;
                }
                
                return node1.degree - node2.degree;
                
            }
        });
        
        // Initialize buckets with 3 nodes each
        int numToDistribute = 3*buckets.size();
        if(buckets.get(0).size*buckets.size() < numToDistribute) {
            numToDistribute = buckets.size();
        }
        
        for(int i = 0; i < numToDistribute; ++i) {
            CommunityModel bucket = buckets.get(i%buckets.size());
            //CommunityModel bucket = buckets.get(i/Math.min(buckets.get(0).size, 3));
            NodeInfo nextNode = nodeInfos.remove(nodeInfos.size()-1);
            bucket.nodeInfo.add(nextNode);
            bucket.minDegree = Math.min(nextNode.degree, bucket.minDegree);
            bucket.minTriangles = Math.min(nextNode.numTriangles, bucket.minTriangles);
            bucket.coreSize++;
        }
        
        
        int start = 0;
        List<NodeInfo> remainingNodes = new ArrayList<NodeInfo>();
        while(!nodeInfos.isEmpty()) {
            NodeInfo nextNode = nodeInfos.remove(nodeInfos.size()-1);
            int index = findBucket(buckets, nextNode, start % buckets.size());
            if(index >= 0) {
                addToBucket(buckets.get(index), nextNode);
            } else if(!accommodateToCore(buckets, nextNode)) {
                remainingNodes.add(nextNode);
            }
        }
        
        
        List<NodeInfo> peripheryNodes = new ArrayList<NodeInfo>(remainingNodes);
        
        for(CommunityModel bucket : buckets) {
            int coreSize = bucket.coreSize;
            double density = bucket.coreDensity;
            for(int i = 0; i < bucket.nodeInfo.size(); ++i) {
                NodeInfo nextNode = bucket.nodeInfo.get(i);
                bucket.triangleBudget[i] = nextNode.numTriangles - ((coreSize-1)*(coreSize-2)*Math.pow(density,3)/2.0);
                bucket.degreeBudget[i] = nextNode.degree - (coreSize-1)*density;
            }
            bucket.currentCoreThreshold = bucket.coreSize;
            for(int j = bucket.coreSize-1;
                j >=0 && (bucket.triangleBudget[j] <= 0 || bucket.degreeBudget[j] <= 0);
                --j, --bucket.currentCoreThreshold) {
            }
        }
    
        remainingNodes.clear();
        for(NodeInfo nextNode : peripheryNodes) {
            if (!addToPeriphery(buckets, nextNode)) {
                remainingNodes.add(nextNode);
            }
        }
    
        for(int i = 0; i < buckets.size(); ++i) {
            CommunityModel nextBucket = buckets.get(i);
            if(nextBucket.nodeInfo.size() < nextBucket.size) {
                int numMissing = nextBucket.size - nextBucket.nodeInfo.size();
                for(int j = 0; j < numMissing; ++j) {
                    NodeInfo nextNode = remainingNodes.remove(remainingNodes.size()-1);
                    nextBucket.nodeInfo.add(nextNode);
                    nextBucket.degreeBudget[0]--;
                    nextBucket.peripherySize++;
                    nextBucket.peripheryDensity[nextBucket.nodeInfo.size()-1] = 1.0;
                    nextBucket.peripheryCoreThreshold[nextBucket.nodeInfo.size()-1] = 1;
                }
            }
        }
        
        return buckets;
        
    }

    public CorePeripheryCommunityStreamer(GraphStats graphStats, Random random) {
        this.random = random;
        communityModels = new HashMap<Integer, List<CommunityModel>>();
        communities = new ArrayList<Community>();
        this.graphStats = graphStats;
        this.nextCommunityId = 0;

        for(Integer size : graphStats.getCommunitySizes()) {
            System.out.println("Generating models for size: "+size);
    
            EmpiricalDistribution degreeDistribution = graphStats.getDegreeDistribution(size);
            int sequenceSize = NUM_MODELS_PER_SIZE*size;
            List<Integer> degree = new ArrayList<Integer>();
            List<Double> clusteringCoefficient = new ArrayList<Double>();
            for (int i = 0; i < sequenceSize; ++i) {
                double nextDegree = degreeDistribution.getNext();
                degree.add((int)nextDegree);
                clusteringCoefficient.add(graphStats.getCCPerDegreeDistribution((int)nextDegree).getNext());
            }
            List<CommunityModel> models = createModels(size,
                                                       NUM_MODELS_PER_SIZE,
                                                       degree,
                                                       clusteringCoefficient);
            communityModels.put(size, models);
        }
    }
    
    
    @Override
    public Community getModel(int id) {
        return null;
    }
    
    @Override
    public Community next() {
        int nextCommunitySize = (int) graphStats.getCommunitySizeDistribution().getNext();
        CommunityModel model = null;
        model = communityModels.get(nextCommunitySize).get(random.nextInt(NUM_MODELS_PER_SIZE));
        Community community = model.generate(nextCommunityId);
        nextCommunityId+=1;
        return community;
    }
}
*/
