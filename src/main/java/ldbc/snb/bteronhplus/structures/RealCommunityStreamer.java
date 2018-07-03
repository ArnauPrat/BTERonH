package ldbc.snb.bteronhplus.structures;

import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class RealCommunityStreamer implements CommunityStreamer {
    
    private ArrayList<Community> models;
    private GraphStats stats;
    private Random random;
    
    public RealCommunityStreamer(GraphStats stats,
                                 String communitiesFile,
                                 String degreeFile,
                                 Random random)  {
        this.random = random;
        this.stats = stats;
        HashMap<Integer, Integer> degrees =  new HashMap<Integer, Integer>();
        Configuration conf  = new Configuration();
        this.models = new ArrayList<Community>();
        long totalObservedEdges = 0;
        long totalExcessDegree = 0;
        long totalObservedNodes = 0;
        try {
    
            ArrayList<Double> communitySizes = new ArrayList<Double>();
            BufferedReader reader = FileTools.getFile(degreeFile, conf);
            String line;
            line = reader.readLine();
            while (line != null) {
                String[] node = line.split("\\|");
                degrees.put(Integer.parseInt(node[0]), Integer.parseInt(node[1]));
                line = reader.readLine();
            }
            reader.close();
            
            int counter = 0;
            reader = FileTools.getFile(communitiesFile, conf);
            line = reader.readLine();
            while (line != null) {
                HashMap<Integer, Integer> idMap = new HashMap<Integer, Integer>();
                HashMap<Integer, Integer> localDegrees = new HashMap<Integer,Integer>();
                int nextId = 0;
                ArrayList<Edge> edges = new ArrayList<Edge>();
                String[] community = line.split("\\|");
                for (int i = 0; i < community.length; ++i) {
                    String[] endpoints = community[i].split(":");
                    int tail = Integer.parseInt(endpoints[0]);
                    int head = Integer.parseInt(endpoints[1]);
                    if(idMap.get(tail) == null) {
                        idMap.put(tail, nextId);
                        localDegrees.put(nextId, degrees.get(tail));
                        nextId++;
                    }
                    tail = idMap.get(tail);
    
                    if(idMap.get(head) == null) {
                        idMap.put(head, nextId);
                        localDegrees.put(nextId, degrees.get(head));
                        nextId++;
                    }
                    head = idMap.get(head);
                    
                    edges.add(new Edge(tail, head));
    
                    totalObservedEdges++;
                }
    
                Map<Integer, Integer > degree = new HashMap<Integer, Integer>();
                for(Edge edge : edges) {
                    degree.merge((int)edge.getTail(), 1 , Integer::sum);
                    degree.merge((int)edge.getHead(), 1 , Integer::sum);
                }
    
                int size = degree.keySet().size();
                int excessDegree[] = new int[size];
                for(Map.Entry<Integer,Integer> entry : degree.entrySet()) {
                    Integer nodeDegree = localDegrees.get(entry.getKey());
                    int localId = entry.getKey();
                    excessDegree[localId] = nodeDegree - entry
                        .getValue();
                    if(excessDegree[localId] < 0) {
                        throw new RuntimeException("Node with excess degree < 0");
                    }
                    totalExcessDegree+=excessDegree[localId];
                }
                
                models.add(new Community(counter, size, edges, excessDegree));
                communitySizes.add((double)size);
                totalObservedNodes+=size;
                line = reader.readLine();
                counter++;
            }
            reader.close();
            System.out.println("Total size in communities models: "+totalObservedNodes);
            System.out.println("Total degree in communities models: "+(totalObservedEdges*2+totalExcessDegree));
        } catch (IOException e ) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    @Override
    public Community getModel(int id) {
        return models.get(id);
    }
    
    @Override
    public Community next() {
        Community community = models.get(random.nextInt(models.size()));
        return community;
    }
    
}
