package ldbc.snb.bteronhplus;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import ldbc.snb.bteronhplus.algorithms.Generator;
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

        @Parameter(names = {"-o", "--output"}, description = "The output file", required = true)
        public String outputFileName;

        @Parameter(names = {"-s", "--size"}, description = "The size of the graph", required = true)
        public int graphSize;

        @Parameter(names = {"-cs", "--csprefix"}, description = "The community structure file name prefix", required =
                true)
        public String communityStructureFileNamePrefix;

        @Parameter(names = {"-l", "--levels"}, description = "The number of community structure levels " )
        public int levels = 2;
    
        @Parameter(names = {"-m", "--modules"}, description = "The modules file prefix", required = true)
        public String modulesPrefix;
    
        @Parameter(names = {"-b", "--basic"}, description = "Use the basic community streamer")
        public boolean basic = false;
    
        @Parameter(names = {"-r", "--random"}, description = "Generate random inter-community structure")
        public boolean random = false;
    
        @Parameter(names = {"-t", "--threshold"}, description = "Generate random inter-community structure")
        public int threshold = -1;
        
        


    }

    public static void main(String[] args) throws Exception {

        Arguments arguments = new Arguments();
        new JCommander(arguments, args);
    
        System.out.println("Modules prefix: "+arguments.modulesPrefix);


        byte[] byteArray = Files.readAllBytes(Paths.get(arguments.communityStructureFileNamePrefix+1));
        String blockModelData = new String(byteArray);
        byteArray = Files.readAllBytes(Paths.get(arguments.communityStructureFileNamePrefix+1+".children"));
        String childrenData = new String(byteArray);
        BlockModel blockModel = new BlockModel(blockModelData, childrenData);

        Random random = new Random();
        //random.setSeed(12345L);
        CommunityStreamer communityStreamer = null;
        if(arguments.basic) {
            communityStreamer = new BasicCommunityStreamer(arguments.modulesPrefix+ "communities", arguments.threshold);
    
        } else {
            communityStreamer = new RealCommunityStreamer(arguments.modulesPrefix + "communities", arguments.threshold);
        }
        
        List<Map<Integer,Long>> partition = Partitioning.partition(random, blockModel,
                                                                   communityStreamer,
                                                                   arguments.graphSize,
                                                                   arguments.graphSize,
                                                                   0,
                                                                   1);
    
        System.out.println("Generating community edges");
        FileWriter outputFile = new FileWriter(arguments.outputFileName);
        FileEdgeWriter writer = new FileEdgeWriter(outputFile);
        
        Generator.sampleEdges(random,
                              writer,
                              blockModel,
                              partition,
                              communityStreamer,
                              0,
                              1,
                              arguments.random);
        writer.close();
    
        Partitioning.printStats(blockModel, partition, communityStreamer);
        
        
    }
}
