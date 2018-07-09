package ldbc.snb.bteronhplus.hadoop;

import ldbc.snb.bteronhplus.structures.Edge;
import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by aprat on 16/08/16.
 */
public class HadoopBTERPlusGenerator {


    public static class HadoopBTERPlusGeneratorMapper  extends Mapper<LongWritable, Text, LongWritable, LongWritable> {


        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            /*int threadId = Integer.parseInt(value.toString());

            Configuration conf = context.getConfiguration();
            String bterPlusStatsFile = conf.get("ldbc.snb.bteronh.generator.bterplusstats");
            String offsetsFile = conf.get("ldbc.snb.bteronh.generator.offsets");
            int numThreads = conf.getInt("ldbc.snb.bteronh.generator.numThreads",1);
            int seed = conf.getInt("ldbc.snb.bteronh.generator.seed",0);

            BTERPlusStats bterPlusStats[] = FileTools.deserializeObjectArray(bterPlusStatsFile,conf);
            System.out.println("Number of plus stats "+bterPlusStats.length);
            Long offsets[] = FileTools.deserializeObjectArray(offsetsFile,conf);

            long totalExternalWeight = 0;
            for(int i = 0; i < bterPlusStats.length; ++i) {
                totalExternalWeight += bterPlusStats[i].totalExternalWeight;
            }

            double cumExternalDegreeProb[] = new double[bterPlusStats.length];
            cumExternalDegreeProb[0] = bterPlusStats[0].totalExternalWeight / (double)totalExternalWeight;
            for(int i = 1; i < bterPlusStats.length; ++i) {
                cumExternalDegreeProb[i] = cumExternalDegreeProb[i-1] + bterPlusStats[i].totalExternalWeight / (double)totalExternalWeight;
            }

            long edgesToGenerate = (totalExternalWeight / 2);
            long blockSize =  edgesToGenerate / numThreads; // Dividing by two because externalWeightPerDegree is
            // degree
            if(threadId == 0) {
                long residual = edgesToGenerate % numThreads;
                blockSize += residual;
            }

            System.out.println("Mapper "+threadId+": Generating "+blockSize+" edges out of "+totalExternalWeight);
            Random random = new Random();
            random.setSeed(seed+threadId);
            for(long i = 0; i < blockSize; ++i) {

                int firstCommunity = BTER.sampleCumulative(cumExternalDegreeProb,random);
                int secondCommunityIndex = BTER.sampleCumulative (bterPlusStats[firstCommunity]
                        .externalWeightPerBlockCumProb,random);
                int secondCommunity = bterPlusStats[firstCommunity].blockIdMap[secondCommunityIndex];

                int firstNodeDegree = BTER.sampleCumulative(bterPlusStats[firstCommunity]
                                .externalWeightPerDegreeCumProb, random) ;

                long firstNode = bterPlusStats[firstCommunity].cumNumNodesPerDegree[firstNodeDegree] +
                        offsets[firstCommunity] + (long)(random.nextDouble()*bterPlusStats[firstCommunity]
                        .numNodesPerDegree[firstNodeDegree]);

                int secondNodeDegree = BTER.sampleCumulative(bterPlusStats[secondCommunity]
                                .externalWeightPerDegreeCumProb, random) ;

                long secondNode = bterPlusStats[secondCommunity].cumNumNodesPerDegree[secondNodeDegree] +
                        offsets[secondCommunity] + (long)(random.nextDouble()*bterPlusStats[secondCommunity]
                        .numNodesPerDegree[secondNodeDegree]);

                Edge edge = new Edge(firstNode,secondNode);


                context.write(new LongWritable(edge.getTail()), new LongWritable(edge.getHead()));
                if( i % 100000 == 0 ) {
                    context.setStatus("Generated "+i+" edges out of "+blockSize+" in mapper "+threadId);
                }
            }
            */
        }
    }


    public static class HadoopBTERPlusGeneratorReducer extends Reducer<LongWritable,LongWritable, LongWritable,
            LongWritable> {

        @Override
        public void reduce(LongWritable tail, Iterable<LongWritable> valueSet,
                           Context context) throws IOException, InterruptedException {
            /*Set<Long> neighbors = new HashSet<Long>();
            for ( LongWritable head : valueSet ) {
                neighbors.add(head.get());
            }

            for( Long head : neighbors) {
                //String str = new String(tail + " " + head + "\n");
                context.write(tail,new LongWritable(head));
            }
            */
        }
    }

}
