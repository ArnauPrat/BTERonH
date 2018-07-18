package ldbc.snb.bteronhplus;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import ldbc.snb.bteronhplus.algorithms.Partitioning;
import ldbc.snb.bteronhplus.hadoop.HadoopCommunityPartitioner;
import ldbc.snb.bteronhplus.hadoop.HadoopEdgeSampler;
import ldbc.snb.bteronhplus.structures.*;
import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
        System.out.println("Partitioning Graph");
        long start = System.currentTimeMillis();
        HadoopCommunityPartitioner communityPartitioner = new HadoopCommunityPartitioner();
        communityPartitioner.run(conf);
        long end = System.currentTimeMillis();
        System.out.println("Graph Partitioned in "+(end - start)+" ms");
    
        /** Checking partition quality **/
        String blockModelData = FileTools.readFile(FileTools.getFile(conf.get("blockModelFilePrefix"), conf));
        String childrenData = FileTools.readFile(FileTools.getFile(conf.get("blockModelFilePrefix")+".children",conf));
        BlockModel blockModel = new BlockModel(blockModelData, childrenData);
    
        LongWritable blockId = new LongWritable();
        HadoopCommunityPartitioner.ModelCountWritable modelCount = new HadoopCommunityPartitioner
            .ModelCountWritable();
    
        // Retrieve partition
        List<Map<Integer, Long>> partition = new ArrayList<Map<Integer,Long>>();
        for(int i = 0; i < blockModel.getNumBlocks(); ++i) {
            partition.add(new HashMap<Integer,Long>());
        }
    
        for(int i = 0; i < conf.getInt("numThreads",1); ++i) {
            String partitionFile = conf.get("partitionFile") + "/part-r-0000"+i;
            SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                                                                 SequenceFile.Reader.file(new Path(partitionFile)));
        
            while (reader.next(blockId, modelCount)) {
                Map<Integer, Long> count = partition.get((int) blockId.get());
                count.merge(modelCount.modelId, modelCount.count, Long::sum);
            }
        }
    
        RealCommunityStreamer streamer = new RealCommunityStreamer(conf.get("communitiesFile"));
    
        Partitioning.printStats(blockModel, partition, streamer);
        /*******/
    
        System.out.println("Sampling Edges");
        start = System.currentTimeMillis();
        HadoopEdgeSampler edgeSampler = new HadoopEdgeSampler();
        edgeSampler.run(conf);
        end = System.currentTimeMillis();
        System.out.println("Edges sampled in "+(end - start)+" ms");
    
        
    }
}
