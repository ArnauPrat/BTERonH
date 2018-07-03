package ldbc.snb.bteronhplus.algorithms;

import ldbc.snb.bteronhplus.structures.BlockModel;
import ldbc.snb.bteronhplus.structures.SuperNode;
import ldbc.snb.bteronhplus.structures.CommunityCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SuperNodeUtils {

    /*private static CommunityCluster buildSuperNodeClusterHierarchy(List<BlockModel> blockModelHierarchy,
                                                                   Map<Integer, CommunityCluster> clusters,
                                                                   int id,
                                                                   List<Integer> children,
                                                                   int level
                                                                   ) {
        if(level == blockModelHierarchy.size()) {
            return clusters.get(id);

        } else {

            BlockModel blockModel = blockModelHierarchy.get(level);

            List<SuperNode> childrenClusters = new ArrayList<SuperNode>();
            for(Integer child : children ) {
                List<Integer> subchildren = blockModel.getChildren().get(child);
                CommunityCluster cluster = buildSuperNodeClusterHierarchy(blockModelHierarchy,
                                                                          clusters,
                                                                          child,
                                                                          subchildren,
                                                                          level+1);
                if(cluster != null) {
                    childrenClusters.add(cluster);
                }
            }
    
            if(childrenClusters.size() == 0) {
                return null;
            }

            double internalDegree = 0;
            double totalDegree = 0;

            for(SuperNode child1 : childrenClusters) {
                BlockModel.ModelEntry entry = blockModel.getEntries().get(child1.getId());
                totalDegree += entry.totalDegree;
                for(SuperNode child2 : childrenClusters) {
                    Double degree = entry.degree.get(child2.getId());
                    if(degree != null) {
                        internalDegree += degree;
                    }
                }
            }

            double externalRatio = 1.0 - internalDegree / (totalDegree);

            return new CommunityCluster(id, childrenClusters, externalRatio);

        }

    }

    public static CommunityCluster buildSuperNodeClusterHierarchy(List<BlockModel> blockModelHierarchy,
                                                                  Map<Integer,CommunityCluster> clusters) {


        BlockModel root = blockModelHierarchy.get(0);

        List<SuperNode> children = new ArrayList<SuperNode>();
        for(Map.Entry<Integer,List<Integer>> entry : root.getChildren().entrySet() ) {
            CommunityCluster cluster = buildSuperNodeClusterHierarchy(blockModelHierarchy,
                                                                      clusters,
                                                                      entry.getKey(),
                                                                      entry.getValue(),
                                                                      1);
            if(cluster != null) {
                children.add(cluster);
            }
        }
        
        if(children.size() == 0) {
            return null;
        }

        return new CommunityCluster(0, children, 0.0);
    }
    */
}
