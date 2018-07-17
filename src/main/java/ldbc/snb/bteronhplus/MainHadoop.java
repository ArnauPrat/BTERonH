package ldbc.snb.bteronhplus;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import ldbc.snb.bteronhplus.hadoop.HadoopCommunityPartitioner;
import ldbc.snb.bteronhplus.hadoop.HadoopEdgeSampler;
import ldbc.snb.bteronhplus.structures.*;
import org.apache.hadoop.conf.Configuration;


public class MainHadoop {

    public static class Arguments {

        @Parameter(names = {"-o", "--output"}, description = "The output file", required = true)
        public String outputFileName;

        @Parameter(names = {"-s", "--size"}, description = "The size of the graph", required = true)
        public int graphSize;

        @Parameter(names = {"-c", "--communities"}, description = "The communities", required =
                true)
        public String communitiesFile;

        @Parameter(names = {"-l", "--levels"}, description = "The number of community structure levels " )
        public int levels = 2;
    
        @Parameter(names = {"-b", "--blockmodel"}, description = "The BlockModel file prefix", required = true)
        public String blockModelPrefix;
    
        @Parameter(names = {"-t", "--threads"}, description = "The number of threads to spawn")
        public int numThreads = 1;


    }

    public static void main(String[] args) throws Exception {

        Arguments arguments = new Arguments();
        new JCommander(arguments, args);
    
        System.out.println("Size: "+arguments.graphSize);
        System.out.println("Communities file: "+arguments.communitiesFile);
        System.out.println("Modules prefix: "+arguments.blockModelPrefix);

    
        Configuration conf = new Configuration();
        if (conf.get("fs.defaultFS").compareTo("file:///") == 0) {
            System.out.println("Running in standalone mode. Setting numThreads to 1");
            conf.set("numThreads", "1");
        } else {
            conf.set("numThreads", Integer.toString(arguments.numThreads));
        }
        conf.set("partitionFile", "partition.dat");
        conf.set("blockModelFilePrefix", arguments.blockModelPrefix+1);
        conf.set("communitiesFile", arguments.communitiesFile);
        conf.setInt("numNodes", arguments.graphSize);
        conf.set("outputFile", arguments.outputFileName);
        HadoopCommunityPartitioner communityPartitioner = new HadoopCommunityPartitioner();
        communityPartitioner.run(conf);
        
        HadoopEdgeSampler edgeSampler = new HadoopEdgeSampler();
        edgeSampler.run(conf);
        
    }
}
