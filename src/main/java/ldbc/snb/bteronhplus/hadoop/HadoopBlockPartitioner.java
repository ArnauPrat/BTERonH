package ldbc.snb.bteronhplus.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by aprat on 16/08/16.
 */
public class HadoopBlockPartitioner extends Partitioner<LongWritable,HadoopCommunityPartitioner.ModelCountWritable> {
    @Override
    public int getPartition(LongWritable tail,
                            HadoopCommunityPartitioner.ModelCountWritable counts, int i) {
        return (int)(tail.get() % i);
    }
}

