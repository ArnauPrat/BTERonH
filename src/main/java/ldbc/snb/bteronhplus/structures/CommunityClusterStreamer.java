package ldbc.snb.bteronhplus.structures;

import java.util.Iterator;
import java.util.List;

public class CommunityClusterStreamer implements CommunityStreamer {

    private List<SuperNode> clusters;
    private Iterator<SuperNode> iter;

    public CommunityClusterStreamer(List<SuperNode> clusters) {
        this.clusters = clusters;
        this.iter = clusters.iterator();
    }
    
    @Override
    public Community getModel(int id) {
        return null;
    }
    
    @Override
    public Community next() {
        return null;
        //return iter.next();
    }
}
