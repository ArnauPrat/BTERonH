package ldbc.snb.bteronhplus.structures;

import org.jgrapht.Graph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.alg.interfaces.SpanningTreeAlgorithm;
import org.jgrapht.alg.spanning.BoruvkaMinimumSpanningTree;
import org.jgrapht.graph.AsSubgraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.jgrapht.graph.builder.GraphBuilder;

import java.util.*;

public class BasicCommunityStreamer implements CommunityStreamer {

    protected static int NUM_MODELS_PER_SIZE = 10;

    private Map<Integer, List<CommunityModel>>  communityModels = null;
    private List<Community>                     communities = null;
    private GraphStats                          graphStats = null;
    private Random                              random = null;
    private int                                 nextCommunityId = 0;

    private static class Stub {
        public int id;
        public int degreeLeft;
        public int degreeUsed;
        public int totalDegree;
    }

    private static class CommunityModel {

        private List<Double>    degrees;
        private double          expectedDensity;
        private double          finalDensity;
        private int             excessDegree[];


        public CommunityModel(List<Double> degrees, double expectedDensity) {
            Collections.sort(degrees, new Comparator<Double>() {
                @Override
                public int compare(Double a, Double b) {
                    return ((int)(double)b) - ((int)(double)a);
                }
            });
            this.degrees = degrees;
            this.expectedDensity = expectedDensity;
            this.finalDensity = 0.0;
        }

        public Community generate(int id) {

            Random random = new Random();

            Stub stubs[] = new Stub[degrees.size()];
            for (int i = 0; i < degrees.size(); ++i) {
                stubs[i] = new Stub();
                stubs[i].id = i;
                stubs[i].degreeLeft = (int)(double)degrees.get(i);
                stubs[i].degreeUsed = 0;
                stubs[i].totalDegree = (int)(double)degrees.get(i);
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
                }
            }

            int numNodes = (int)(double)degrees.size();
            int requiredEdges = (int)(numNodes*numNodes*expectedDensity)/2;
            GraphBuilder builder = SimpleGraph.createBuilder(DefaultEdge.class);
            for(Edge edge : edges) {
                builder.addEdge(edge.getTail(), edge.getHead());
            }
            Graph<Long,DefaultEdge> graph = builder.build();
            ConnectivityInspector<Long,DefaultEdge> connectivityInspector = new ConnectivityInspector<>(graph);
            List<Set<Long>> connectedComponents = connectivityInspector.connectedSets();
            while(connectedComponents.size() > 1 ) {

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

                if(candidateEdges1.size() == 0 && candidateEdges2.size() == 0) {
                    return null;
                }

                ArrayList<DefaultEdge> edgesArray1 = null;
                if(candidateEdges1.size() > 0) {
                    edgesArray1 = new ArrayList<DefaultEdge>(candidateEdges1);
                } else {
                    edgesArray1 = new ArrayList<DefaultEdge>(spanningTree1.getEdges());
                }
                ArrayList<DefaultEdge> edgesArray2 = null;

                if(candidateEdges2.size() > 0) {
                    edgesArray2 = new ArrayList<DefaultEdge>(candidateEdges2);
                } else {
                    edgesArray2 = new ArrayList<>(spanningTree2.getEdges());
                }

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

            spanningTreeAlgorithm = new BoruvkaMinimumSpanningTree(graph);
            spanningTree = spanningTreeAlgorithm.getSpanningTree();
            candidateEdges = new HashSet<DefaultEdge>(graph.edgeSet());
            candidateEdges.removeAll(spanningTree.getEdges());
            ArrayList<DefaultEdge> toRemove = new ArrayList<DefaultEdge>(candidateEdges);
            Collections.shuffle(toRemove);
            int edgesToRemove = Math.max(graph.edgeSet().size() - requiredEdges,1);
            while(!toRemove.isEmpty() && edgesToRemove > 0) {
                graph.removeEdge(toRemove.get(toRemove.size()-1));
                toRemove.remove(toRemove.size()-1);
                edgesToRemove--;
            }

            stubs = new Stub[degrees.size()];

            for (int i = 0; i < degrees.size(); ++i) {
                stubs[i] = new Stub();
                stubs[i].id = i;
                stubs[i].degreeLeft = (int)(double)degrees.get(i);
                stubs[i].degreeUsed = 0;
                stubs[i].totalDegree = (int)(double)degrees.get(i);
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


            int excessDegree[] = new int[degrees.size()];
            for(int i = 0; i < stubs.length; ++i ) {
                excessDegree[i]=stubs[i].degreeLeft;
            }

            return new Community(id, degrees.size(), edges, excessDegree);
        }

        public List<Double> getDegrees() {
            return degrees;
        }
    }

    public BasicCommunityStreamer(GraphStats graphStats) {
        communityModels = new HashMap<Integer, List<CommunityModel>>();
        communities = new ArrayList<Community>();
        this.graphStats = graphStats;
        this.random = new Random();
        this.nextCommunityId = 0;

        for(Integer size : graphStats.getCommunitySizes()) {
            System.out.println("Generating models for size: "+size);
            ArrayList<CommunityModel> models = new ArrayList<CommunityModel>();
            while(models.size() < NUM_MODELS_PER_SIZE) {
                EmpiricalDistribution degreeDistribution = graphStats.getDegreeDistribution(size);
                List<Double> degrees = new ArrayList<Double>();
                for (int i = 0; i < size; ++i) {
                    double nextDegree = degreeDistribution.getNext();
                    degrees.add(nextDegree);
                }

                double density = (double) graphStats.getCommunityDensity(size);
                CommunityModel model = new CommunityModel(degrees, density);

                Community community = model.generate(nextCommunityId);
                if(community != null) {
                    models.add(model);
                }
            }
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
