package ldbc.snb.bteronhplus.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by aprat on 16/08/16.
 */
public class HadoopEdgePartitioner extends Partitioner<LongWritable,LongWritable> {
    
    @Override
    public int getPartition(LongWritable tail, LongWritable head, int i) {
        return (int)(tail.get() % i);
    }
}
