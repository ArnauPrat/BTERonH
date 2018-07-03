package ldbc.snb.bteronhplus;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import ldbc.snb.bteronhplus.algorithms.Partitioning;
import ldbc.snb.bteronhplus.structures.*;
import org.jgrapht.Graph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.jgrapht.graph.builder.GraphBuilder;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class Main {

    public static class Arguments {

        @Parameter(names = {"-d", "--degrees"}, description = "The file with the degrees", required = true)
        public String degreesFile;

        @Parameter(names = {"-cc", "--clustering"}, description = "The file with the clustering coefficient " +
                "distributions per degree", required = true)
        public String ccsFile;

        @Parameter(names = {"-c", "--communities"}, description = "The file with the community sizes", required = true)
        public String communitiesfile;

        @Parameter(names = {"-o", "--output"}, description = "The output file", required = true)
        public String outputFileName;

        @Parameter(names = {"-p", "--density"}, description = "The community densities file", required = true)
        public String densityFileName;

        @Parameter(names = {"-s", "--size"}, description = "The size of the graph", required = true)
        public int graphSize;

        @Parameter(names = {"-cs", "--csprefix"}, description = "The community structure file name prefix", required =
                true)
        public String communityStructureFileNamePrefix;

        @Parameter(names = {"-l", "--levels"}, description = "The number of community structure levels " )
        public int levels = 2;
    
        @Parameter(names = {"-m", "--modules"}, description = "The modules file prefix", required = true)
        public String modulesPrefix;


    }

    public static void main(String[] args) throws Exception {

        Arguments arguments = new Arguments();
        new JCommander(arguments, args);
    
        System.out.println("Degree file: "+arguments.degreesFile);
        System.out.println("CCS file: "+arguments.ccsFile);
        System.out.println("Communities file: "+arguments.communitiesfile);
        System.out.println("Density file: "+arguments.densityFileName);
        System.out.println("Modules prefix: "+arguments.modulesPrefix);

        GraphStats graphStats = new GraphStats(arguments.degreesFile,
                                               arguments.ccsFile,
                                               arguments.communitiesfile,
                                               arguments.densityFileName);


        byte[] byteArray = Files.readAllBytes(Paths.get(arguments.communityStructureFileNamePrefix+1));
        String blockModelData = new String(byteArray);
        byteArray = Files.readAllBytes(Paths.get(arguments.communityStructureFileNamePrefix+1+".children"));
        String childrenData = new String(byteArray);
        BlockModel blockModel = new BlockModel(blockModelData, childrenData);

        Random random = new Random();
        random.setSeed(12345L);

        //BasicCommunityStreamer communityStreamer = new BasicCommunityStreamer(graphStats);
        //CorePeripheryCommunityStreamer communityStreamer = new CorePeripheryCommunityStreamer(graphStats,random);
        RealCommunityStreamer communityStreamer = new RealCommunityStreamer(graphStats, arguments.modulesPrefix+
            "communities", arguments.modulesPrefix+"degreemap", random);
        List<Map<Integer,Long>> partition = Partitioning.partition(random, blockModel,
                                                                       communityStreamer,
                                                                       arguments.graphSize);
        
        // Counting total number of nodes
        long totalDegree = 0L;
        long totalExternalDegree = 0L;
        long totalNumNodes = 0L;
        long internalDegree[] = new long[partition.size()];
        Arrays.fill(internalDegree, 0L);
        
        int index = 0;
        for(Map<Integer,Long> counts : partition) {
            for(Map.Entry<Integer,Long> entry : counts.entrySet()) {
                Community model = communityStreamer.getModel(entry.getKey());
                totalNumNodes += entry.getValue()*model.getSize();
                totalDegree += entry.getValue()*(model.getExternalDegree()+model.getInternalDegree());
                totalExternalDegree += entry.getValue()*(model.getExternalDegree());
                internalDegree[index] += entry.getValue()*model.getInternalDegree();
            }
            index++;
        }
    
        System.out.println("Generating community edges");
        FileWriter outputFile = new FileWriter(arguments.outputFileName);
        
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
                
                if(entry.id == neighbor.getKey()) {
    
                    sampler.generateCommunityEdges(outputFile,
                                                    partition.get(entry.id),
                                                    communityStreamer,
                                                    offsets[entry.id],
                                                    builder);
    
                    long numEdges = (Math.round(neighbor.getValue() * totalDegree) - internalDegree[entry.id])/2;
    
    
                    if(numEdges < (sampler.getNumCommunities()-1) && sampler.getNumCommunities() > 1){
                        totalNumEdgesBelowThreshold++;
                        if(numEdges < 0) totalNumEdgesNegative++;
                    } else {
                        numCommunitiesIfWellConnected++;
                    }
    
                    numEdges -= sampler.getNumCommunities() - 1;
                    sampler.generateConnectedGraph(outputFile, random, offsets[entry.id], builder);
                    
                    for(int i = 0; i < numEdges; ++i) {
                        long node1 = sampler.sample(random, offsets[entry.id]);
                        long node2 = sampler.sample(random, offsets[entry.id]);
                        totalExpectedGeneratedEdges++;
                        if (node1 != -1 && node2 != -1) {
                            outputFile.write(node1 + "\t" + node2 + "\n");
    
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
    
                    for(int i = 0; i < numEdges; ++i) {
                        long node1 = sampler1.sample(random, offsets[entry.id]);
                        long node2 = sampler2.sample(random, offsets[entry.id]);
                        totalExpectedGeneratedEdges++;
                        if (node1 != -1 && node2 != -1) {
                            outputFile.write(node1 + "\t" + node2 + "\n");
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
    
        outputFile.close();
        
    }
}
