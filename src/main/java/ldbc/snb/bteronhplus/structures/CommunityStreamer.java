package ldbc.snb.bteronhplus.structures;

import java.util.Random;

public interface CommunityStreamer {
    
    public abstract Community getModel(int id);

    public abstract Community next(Random random);

}
