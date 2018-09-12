package ldbc.snb.bteronhplus.structures;

import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.hadoop.conf.Configuration;
import org.jgrapht.Graph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.alg.interfaces.SpanningTreeAlgorithm;
import org.jgrapht.alg.spanning.BoruvkaMinimumSpanningTree;
import org.jgrapht.graph.AsSubgraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.jgrapht.graph.builder.GraphBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class BasicCommunityStreamer implements CommunityStreamer {
    
    protected static int NUM_MODELS_PER_SIZE = 10;
    
    private Map<Integer, List<CommunityModel>>  communityModels = null;
    private List<Community>                     communities = null;
    
    private static class Stub {
        public int id;
        public int degreeLeft;
        public int degreeUsed;
        public int totalDegree;
    }
    
    private static class CommunityModel {
        
        private List<Integer>   degrees;
        private List<Integer>   excessDegree;
        private List<Double>    clusteringCoefficient;
        
        
        public CommunityModel(List<Integer> degrees,
                              List<Integer> excessDegree,
                              List<Double> clusteringCoefficient) {
            this.degrees = degrees;
            this.excessDegree = excessDegree;
            this.clusteringCoefficient = clusteringCoefficient;
        }
        
        public Community generate(int id) {
            
            Random random = new Random();
            
            Stub stubs[] = new Stub[degrees.size()];
            for (int i = 0; i < degrees.size(); ++i) {
                stubs[i] = new Stub();
                stubs[i].id = i;
                stubs[i].degreeLeft = degrees.get(i) - excessDegree.get(i);
                stubs[i].degreeUsed = 0;
                stubs[i].totalDegree = stubs[i].degreeLeft;
            }
            
            Arrays.sort(stubs, new Comparator<Stub>() {
                @Override
                public int compare(Stub pair1, Stub pair2) {
                    return pair2.degreeLeft - pair1.degreeLeft;
                }
            });
            
            List<Edge> edges = new ArrayList<Edge>();
            
            for(int i = 0; i < stubs.length; ++i) {
                for(int j = i+1; j < stubs.length; ++j) {
                    if(stubs[i].degreeLeft > 0 && stubs[j].degreeLeft > 0) {
                        Edge edge = new Edge(stubs[i].id, stubs[j].id);
                        stubs[i].degreeLeft--;
                        stubs[i].degreeUsed++;
                        stubs[j].degreeLeft--;
                        stubs[j].degreeUsed++;
                        edges.add(edge);
                    }
                    
                    if(stubs[i].degreeLeft == 0) {
                        break;
                    }
                }
            }
            
            /*
            GraphBuilder builder = SimpleGraph.createBuilder(DefaultEdge.class);
            for(Edge edge : edges) {
                builder.addEdge(edge.getTail(), edge.getHead());
            }
            Graph<Long,DefaultEdge> graph = builder.build();
            ConnectivityInspector<Long,DefaultEdge> connectivityInspector = new ConnectivityInspector<>(graph);
            List<Set<Long>> connectedComponents = connectivityInspector.connectedSets();
            boolean finish = false;
            while(connectedComponents.size() > 1 && !finish) {
                finish = true;
                
                connectedComponents.sort(new Comparator<Set<Long>>() {
                    @Override
                    public int compare(Set<Long> component1, Set<Long> component2) {
                        return component2.size() - component1.size();
                    }
                });
                
                AsSubgraph<Long,DefaultEdge> first = new AsSubgraph<>(graph,connectedComponents.get(0));
                AsSubgraph<Long,DefaultEdge> second = new AsSubgraph<>(graph,connectedComponents.get(1));
                
                BoruvkaMinimumSpanningTree spanningTreeAlgorithm1 = new BoruvkaMinimumSpanningTree(first);
                SpanningTreeAlgorithm.SpanningTree<DefaultEdge> spanningTree1 = spanningTreeAlgorithm1.getSpanningTree();
                BoruvkaMinimumSpanningTree spanningTreeAlgorithm2 = new BoruvkaMinimumSpanningTree(second);
                SpanningTreeAlgorithm.SpanningTree<DefaultEdge> spanningTree2 = spanningTreeAlgorithm2.getSpanningTree();
                
                Set<DefaultEdge> candidateEdges1 = new HashSet<DefaultEdge>(first.edgeSet());
                candidateEdges1.removeAll(spanningTree1.getEdges());
                
                Set<DefaultEdge> candidateEdges2 = new HashSet<DefaultEdge>(second.edgeSet());
                candidateEdges2.removeAll(spanningTree2.getEdges());
                
                //if(candidateEdges1.size() == 0 && candidateEdges2.size() == 0) {
                //    return null;
                //}
                
                ArrayList<DefaultEdge> edgesArray1 = new ArrayList<DefaultEdge>(candidateEdges1);
                ArrayList<DefaultEdge> edgesArray2 = new ArrayList<DefaultEdge>(candidateEdges2);
                
                Collections.shuffle(edgesArray1);
                Collections.shuffle(edgesArray2);
                
                while(edgesArray1.size() > 0 && edgesArray2.size() > 0) {
                    
                    DefaultEdge edge1 = edgesArray1.get(edgesArray1.size()-1);
                    edgesArray1.remove(edgesArray1.size()-1);
                    
                    DefaultEdge edge2 = edgesArray2.get(edgesArray2.size()-1);
                    edgesArray2.remove(edgesArray2.size()-1);
                    
                    
                    Long source1 = graph.getEdgeSource(edge1);
                    Long target1 = graph.getEdgeTarget(edge1);
                    Long source2 = graph.getEdgeSource(edge2);
                    Long target2 = graph.getEdgeTarget(edge2);
                    
                    graph.removeEdge(edge1);
                    graph.removeEdge(edge2);
                    graph.addEdge(source1, source2);
                    graph.addEdge(target1, target2);
                    
                    finish = false;
                }
                
                connectivityInspector = new ConnectivityInspector<>(graph);
                connectedComponents = connectivityInspector.connectedSets();
                
            }
            
            
            BoruvkaMinimumSpanningTree spanningTreeAlgorithm = new BoruvkaMinimumSpanningTree(graph);
            SpanningTreeAlgorithm.SpanningTree<DefaultEdge> spanningTree = spanningTreeAlgorithm.getSpanningTree();
            Set<DefaultEdge> candidateEdges = new HashSet<DefaultEdge>(graph.edgeSet());
            candidateEdges.removeAll(spanningTree.getEdges());
            ArrayList<DefaultEdge> toShuffle = new ArrayList<DefaultEdge>(candidateEdges);
            Collections.shuffle(toShuffle);
            while(!toShuffle.isEmpty()) {
                DefaultEdge first = toShuffle.get(toShuffle.size()-1);
                toShuffle.remove(toShuffle.size()-1);
                if(!toShuffle.isEmpty()) {
                    DefaultEdge second = toShuffle.get(toShuffle.size()-1);
                    toShuffle.remove(toShuffle.size()-1);
                    
                    Long source1 = graph.getEdgeSource(first);
                    Long target1 = graph.getEdgeTarget(first);
                    Long source2 = graph.getEdgeSource(second);
                    Long target2 = graph.getEdgeTarget(second);
                    Set<Long> set = new HashSet<Long>();
                    set.add(source1);
                    set.add(target1);
                    set.add(source2);
                    set.add(target2);
                    if (set.size() == 4) {
                        graph.removeEdge(first);
                        graph.removeEdge(second);
                        graph.addEdge(source1, source2);
                        graph.addEdge(target1, target2);
                    }
                }
            }
            
            stubs = new Stub[degrees.size()];
            
            for (int i = 0; i < degrees.size(); ++i) {
                stubs[i] = new Stub();
    
                stubs[i].id = i;
                stubs[i].degreeLeft = degrees.get(i) - excessDegree.get(i);
                stubs[i].degreeUsed = 0;
                stubs[i].totalDegree = stubs[i].degreeLeft;
            }
            Set<DefaultEdge> finalEdges = graph.edgeSet();
            
            edges = new ArrayList<Edge>();
            for(DefaultEdge edge : finalEdges ) {
                Long source = graph.getEdgeSource(edge);
                Long target = graph.getEdgeTarget(edge);
                stubs[(int)(long)source].degreeLeft--;
                stubs[(int)(long)source].degreeUsed++;
                
                stubs[(int)(long)target].degreeLeft--;
                stubs[(int)(long)target].degreeUsed++;
                edges.add(new Edge(source,target));
            }
            */
            
            
            ArrayList<Integer> finalExcessDegree = new ArrayList<Integer>();
            finalExcessDegree.ensureCapacity(degrees.size());
            for(int i = 0; i < stubs.length; ++i ) {
                finalExcessDegree.add(excessDegree.get(i) + stubs[i].degreeLeft);
            }
            
            return new Community(id,
                                 finalExcessDegree,
                                 clusteringCoefficient,
                                 edges);
        }
        
        public List<Integer> getDegrees() {
            return degrees;
        }
    }
    
    public BasicCommunityStreamer(String communitiesFile) {
        Configuration conf  = new Configuration();
        communityModels = new HashMap<Integer, List<CommunityModel>>();
        communities = new ArrayList<Community>();
        int nextCommunityId = 0;
        try {
            BufferedReader reader = FileTools.getFile(communitiesFile, conf);
            String         line   = reader.readLine();
            while (line != null) {
                HashMap<Integer, Integer> idMap                 = new HashMap<Integer, Integer>();
                ArrayList<Integer>        graphDegrees          = new ArrayList<Integer>();
                ArrayList<Double>         clusteringCoefficient = new ArrayList<Double>();
                ArrayList<Integer>        excessDegree          = new ArrayList<Integer>();
                ArrayList<Edge>           edges                 = new ArrayList<Edge>();
                String[]                  community             = line.split("\\|");
                String[]                  nodesstr              = community[0].split(" ");
                for (int i = 0; i < nodesstr.length; ++i) {
                    String nodeInfo[] = nodesstr[i].split(":");
                    idMap.put(Integer.parseInt(nodeInfo[0]), i);
                    graphDegrees.add(Integer.parseInt(nodeInfo[1]));
                    clusteringCoefficient.add(Double.parseDouble(nodeInfo[2]));
                    excessDegree.add(0);
                }
    
                //System.out.println("ENTRA: "+nextCommunityId+" "+graphDegrees.size());
                
                /*if (community.length == 2)*/ {
                    
                    String[]              edgesstr = community[1].split(" ");
                    Map<Integer, Integer> degree   = new HashMap<Integer, Integer>();
                    for (int i = 0; i < graphDegrees.size(); ++i) {
                        degree.put(i, 0);
                    }
                    
                    for (int i = 0; i < edgesstr.length; ++i) {
                        String[] endpoints = edgesstr[i].split(":");
                        int      tail      = Integer.parseInt(endpoints[0]);
                        int      head      = Integer.parseInt(endpoints[1]);
                        tail = idMap.get(tail);
                        head = idMap.get(head);
                        Edge edge = new Edge(tail, head);
                        edges.add(edge);
                        degree.merge((int) edge.getTail(), 1, Integer::sum);
                        degree.merge((int) edge.getHead(), 1, Integer::sum);
                    }
                    
                    for (int i = 0; i < graphDegrees.size(); ++i) {
                        Integer localDegree   = graphDegrees.get(i);
                        Integer currentDegree = degree.get(i);
                        excessDegree.set(i, localDegree - currentDegree);
                        if (excessDegree.get(i) < 0) {
                            throw new RuntimeException("Node with excess degree < 0");
                        }
                    }
                    
                    CommunityModel model = new CommunityModel(graphDegrees,
                                                              excessDegree,
                                                              clusteringCoefficient);
                    
                    Community generatedCommunity = null;
                    int count = 0;
                    do {
                        generatedCommunity = model.generate(nextCommunityId);
                        if(generatedCommunity != null) {
                            communities.add(generatedCommunity);
                            nextCommunityId++;
                            break;
                        }
                        count++;
                    } while (generatedCommunity == null && count < 100);
                }
                
                line = reader.readLine();
            }
        } catch (IOException e ) {
            e.printStackTrace();
            System.exit(1);
        }
        
    }
    
    @Override
    public Community getModel(int id) {
        Community community = communities.get(id);
        return community;
    }
    
    @Override
    public Community next(Random random) {
        return communities.get(random.nextInt(communities.size()));
    }
}
