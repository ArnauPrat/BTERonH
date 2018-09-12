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

        /*GraphStats graphStats = new GraphStats(arguments.degreesFile,
                                               arguments.ccsFile,
                                               arguments.communitiesfile,
                                               arguments.densityFileName);
                                               */


        byte[] byteArray = Files.readAllBytes(Paths.get(arguments.communityStructureFileNamePrefix+1));
        String blockModelData = new String(byteArray);
        byteArray = Files.readAllBytes(Paths.get(arguments.communityStructureFileNamePrefix+1+".children"));
        String childrenData = new String(byteArray);
        BlockModel blockModel = new BlockModel(blockModelData, childrenData);

        Random random = new Random();
        random.setSeed(12345L);

        //CorePeripheryCommunityStreamer communityStreamer = new CorePeripheryCommunityStreamer(graphStats,random);
        /*RealCommunityStreamer communityStreamer = new RealCommunityStreamer(arguments.modulesPrefix+
                                                        "communities");
                                                        */
        
        
        BasicCommunityStreamer communityStreamer = new BasicCommunityStreamer(arguments.modulesPrefix+
                                                        "communities");
                                                        
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
                              1);
        writer.close();
    
        Partitioning.printStats(blockModel, partition, communityStreamer);
        
        
    }
}
