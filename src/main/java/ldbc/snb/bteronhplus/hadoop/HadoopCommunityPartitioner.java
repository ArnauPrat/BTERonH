package ldbc.snb.bteronhplus.hadoop;

import ldbc.snb.bteronhplus.algorithms.Partitioning;
import ldbc.snb.bteronhplus.structures.BlockModel;
import ldbc.snb.bteronhplus.structures.GraphStats;
import ldbc.snb.bteronhplus.structures.RealCommunityStreamer;
import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by aprat on 16/08/16.
 */
public class HadoopCommunityPartitioner {
    
    public static class ModelCountWritable implements Writable {
        
        public int modelId;
        public long count;
        
        public ModelCountWritable() {
            int modelId = Integer.MAX_VALUE;
            long count = Long.MAX_VALUE;
        }
        
        public ModelCountWritable(int modelId,
                                  long count) {
            this.modelId = modelId;
            this.count = count;
        }
    
        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(modelId);
            dataOutput.writeLong(count);
        }
    
        @Override
        public void readFields(DataInput dataInput) throws IOException {
            modelId = dataInput.readInt();
            count = dataInput.readLong();
        }
    }


    public static class HadoopCommunityPartitionerMapper  extends Mapper<LongWritable,
                                                                         Text,
                                                                         LongWritable,
                                                                         ModelCountWritable> {


        @Override
        public void map(LongWritable key,
                        Text value,
                        Context context) throws IOException, InterruptedException {
    
            int threadId = Integer.parseInt(value.toString());
            Configuration conf = context.getConfiguration();
            
            int numThreads = conf.getInt("numThreads",1);
            long numNodes = conf.getLong("numNodes",10000L);
            String blockModelFile = conf.get("blockModelFilePrefix");
            String communitiesFileName = conf.get("communitiesFile");
            
    
            
            String blockModelData = FileTools.readFile(FileTools.getFile(blockModelFile,conf));
            String childrenData = FileTools.readFile(FileTools.getFile(blockModelFile+".children",conf));
            BlockModel blockModel = new BlockModel(blockModelData, childrenData);
    
            Random random = new Random();
            random.setSeed(threadId);
            RealCommunityStreamer communityStreamer = new RealCommunityStreamer(communitiesFileName);
            
            long nodesToGenerate = numNodes / numThreads;
    
    
            context.setStatus("Partitioning communities");
            List<Map<Integer,Long>> partition = Partitioning.partition(random,
                                                                       blockModel,
                                                                       communityStreamer,
                                                                       numNodes,
                                                                       nodesToGenerate,
                                                                       threadId,
                                                                       numThreads
                                                                       );
            
            for(int i = 0 ; i < partition.size(); ++i) {
                int blockId = i;
                for(Map.Entry<Integer,Long> entry : partition.get(i).entrySet()) {
                    context.write(new LongWritable(blockId), new ModelCountWritable(entry.getKey(),
                                                                                    entry.getValue()));
                }
            }
            
            context.setStatus("Mapper "+threadId+" finished execution");
    
            
        }
    }


    public static class HadoopCommunityPartitionerReducer extends Reducer<LongWritable,
                                                                          ModelCountWritable,
                                                                          LongWritable,
                                                                          ModelCountWritable> {

        @Override
        public void reduce(LongWritable tail,
                           Iterable<ModelCountWritable> valueSet,
                           Context context) throws IOException, InterruptedException {
            
            Map<Integer, Long> counts = new HashMap<Integer,Long>();
            for(ModelCountWritable modelCount : valueSet) {
                counts.merge(modelCount.modelId, modelCount.count, Long::sum);
            }
            
            for(Map.Entry<Integer,Long> entry : counts.entrySet()) {
                context.write(new LongWritable(tail.get()),
                              new ModelCountWritable(entry.getKey(),entry.getValue()));
            }
        }
    }
    
    public void run(Configuration conf)  throws Exception {
        
        FileSystem dfs = FileSystem.get(conf);
        String triggerFile = "trigger.dat";
        int numThreads = Integer.parseInt(conf.get("numThreads"));
        FileTools.writeToOutputFile(triggerFile,
                                    numThreads,
                                    conf);
        
        String partitionOutputFile = conf.get("partitionFile");

        conf.setInt("mapreduce.input.lineinputformat.linespermap", 1);
        Job job = Job.getInstance(conf, "Partitioning Communities");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ModelCountWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(ModelCountWritable.class);
        
        job.setJarByClass(HadoopCommunityPartitionerMapper.class);
        job.setMapperClass(HadoopCommunityPartitionerMapper.class);
        job.setReducerClass(HadoopCommunityPartitionerReducer.class);
        
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(HadoopBlockPartitioner.class);
        FileInputFormat.setInputPaths(job, new Path(triggerFile));
        FileOutputFormat.setOutputPath(job, new Path(partitionOutputFile));
        if(!job.waitForCompletion(true)) {
            throw new Exception(job.toString());
        }
    
    }
}
