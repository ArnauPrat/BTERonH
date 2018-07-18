package ldbc.snb.bteronhplus.hadoop;

import ldbc.snb.bteronhplus.algorithms.Generator;
import ldbc.snb.bteronhplus.algorithms.Partitioning;
import ldbc.snb.bteronhplus.structures.BlockModel;
import ldbc.snb.bteronhplus.structures.EdgeWriter;
import ldbc.snb.bteronhplus.structures.RealCommunityStreamer;
import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by aprat on 16/08/16.
 */
public class HadoopEdgeSampler {
    
    public static class HadoopEdgeSamplerMapper  extends Mapper<LongWritable,
                                                                         Text,
                                                                         LongWritable,
                                                                         LongWritable> {
        private static class HadoopEdgeWriter implements EdgeWriter {
            
            private Context context = null;
            
            public HadoopEdgeWriter(Context context) {
                this.context = context;
            }
    
            @Override
            public void write(long tail, long head) throws IOException {
                try {
                    context.write(new LongWritable(tail), new LongWritable(head));
                } catch (InterruptedException e) {
                    throw new IOException(e.getMessage());
                }
            }
    
            @Override
            public void close() throws IOException {
        
            }
        }


        @Override
        public void map(LongWritable key,
                        Text value,
                        Context context) throws IOException, InterruptedException {
    
            int threadId = Integer.parseInt(value.toString());
            Configuration conf = context.getConfiguration();
            
            int numThreads = conf.getInt("numThreads", 1);
            String blockModelFile = conf.get("blockModelFilePrefix");
            String communitiesFileName = conf.get("communitiesFile");
    
    
            // Retrieve blockModel
            String blockModelData = FileTools.readFile(FileTools.getFile(blockModelFile,conf));
            String childrenData = FileTools.readFile(FileTools.getFile(blockModelFile+".children",conf));
            BlockModel blockModel = new BlockModel(blockModelData, childrenData);
    
            Random random = new Random();
            random.setSeed(threadId);
            
            // Retrieve partition
            List<Map<Integer, Long>> partition = new ArrayList<Map<Integer,Long>>();
            for(int i = 0; i < blockModel.getNumBlocks(); ++i) {
                partition.add(new HashMap<Integer,Long>());
            }
    
    
            LongWritable blockId = new LongWritable();
            HadoopCommunityPartitioner.ModelCountWritable modelCount = new HadoopCommunityPartitioner
                .ModelCountWritable();
            
            for(int i = 0; i < numThreads; ++i) {
                String partitionFile = conf.get("partitionFile") + "/part-r-0000"+i;
                SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                                                                     SequenceFile.Reader.file(new Path(partitionFile)));
    
                while (reader.next(blockId, modelCount)) {
                    Map<Integer, Long> count = partition.get((int) blockId.get());
                    count.merge(modelCount.modelId, modelCount.count, Long::sum);
                }
            }
            
            // Retrieve communityStreamer
            RealCommunityStreamer streamer = new RealCommunityStreamer(communitiesFileName, random);
            
            // Run generator
    
            HadoopEdgeWriter writer = new HadoopEdgeWriter(context);
    
            Generator.sampleEdges(random,
                                  writer,
                                  blockModel,
                                  partition,
                                  streamer,
                                  threadId,
                                  numThreads);
        }
    }


    public static class HadoopEdgeSamplerReducer extends Reducer<LongWritable,
                                                                          LongWritable,
                                                                          LongWritable,
                                                                          LongWritable> {

        @Override
        public void reduce(LongWritable tail,
                           Iterable<LongWritable> valueSet,
                           Context context) throws IOException, InterruptedException {
            
            Set<Long> edges = new HashSet<Long>();
            for(LongWritable neighbor : valueSet) {
                edges.add(new Long(neighbor.get()));
            }
            
            edges.remove(tail.get());
            
            for(Long neighbor : edges) {
                context.write(new LongWritable(tail.get()),
                              new LongWritable(neighbor));
            }
        }
    }
    
    public void run(Configuration conf)  throws Exception {
        
        FileSystem dfs = FileSystem.get(conf);
        String triggerFile = "trigger.dat";
        FileTools.writeToOutputFile(triggerFile,
                                    Integer.parseInt(conf.get("numThreads")),
                                    conf);
        
        String outputFile = conf.get("outputFile");

        int numThreads = Integer.parseInt(conf.get("numThreads"));
        conf.setInt("mapreduce.input.lineinputformat.linespermap", 1);
        Job job = Job.getInstance(conf, "Sampling Edges");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setJarByClass(HadoopEdgeSamplerMapper.class);
        job.setMapperClass(HadoopEdgeSamplerMapper.class);
        job.setReducerClass(HadoopEdgeSamplerReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(HadoopEdgePartitioner.class);
        FileInputFormat.setInputPaths(job, new Path(triggerFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        if(!job.waitForCompletion(true)) {
            throw new Exception(job.toString());
        }
    
    }
}
