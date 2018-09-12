package ldbc.snb.bteronhplus.structures;

import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class RealCommunityStreamer implements CommunityStreamer {
    
    private ArrayList<Community> models;
    
    public RealCommunityStreamer(String communitiesFile )  {
        Configuration conf  = new Configuration();
        this.models = new ArrayList<Community>();
        long totalObservedEdges = 0;
        long totalExcessDegree = 0;
        long totalObservedNodes = 0;
        try {
            int counter = 0;
            ArrayList<Double> communitySizes = new ArrayList<Double>();
            BufferedReader reader = FileTools.getFile(communitiesFile, conf);
            String line = reader.readLine();
            while (line != null) {
                HashMap<Integer, Integer> idMap = new HashMap<Integer, Integer>();
                ArrayList<Integer> graphDegrees = new ArrayList<Integer>();
                ArrayList<Double>  clusteringCoefficient = new ArrayList<Double>();
                ArrayList<Integer> excessDegree = new ArrayList<Integer>();
                ArrayList<Edge> edges = new ArrayList<Edge>();
                String[] community = line.split("\\|");
                String[] nodesstr = community[0].split(" ");
                for (int i = 0; i < nodesstr.length; ++i) {
                    String nodeInfo[] = nodesstr[i].split(":");
                    idMap.put(Integer.parseInt(nodeInfo[0]), i);
                    graphDegrees.add(Integer.parseInt(nodeInfo[1]));
                    clusteringCoefficient.add(Double.parseDouble(nodeInfo[2]));
                    excessDegree.add(0);
                }
                if (community.length == 2) {
                    
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
                        totalObservedEdges++;
                    }
                    
                    for (int i = 0; i < graphDegrees.size(); ++i) {
                        Integer localDegree   = graphDegrees.get(i);
                        Integer currentDegree = degree.get(i);
                        excessDegree.set(i, localDegree - currentDegree);
                        if (excessDegree.get(i) < 0) {
                            throw new RuntimeException("Node with excess degree < 0");
                        }
                        
                        totalExcessDegree += excessDegree.get(i);
                    }
                    
                    models.add(new Community(counter,
                                             excessDegree,
                                             clusteringCoefficient,
                                             edges));
                    communitySizes.add((double) excessDegree.size());
                    totalObservedNodes += excessDegree.size();
                    counter++;
                }
                line = reader.readLine();
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
    public Community next(Random random) {
        Community community = models.get(random.nextInt(models.size()));
        return community;
    }
    
}
