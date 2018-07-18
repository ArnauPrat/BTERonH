package ldbc.snb.bteronhplus.structures;

import java.io.IOException;

public interface EdgeWriter {
    public void write(long tail, long head) throws IOException;
    
    public void close() throws IOException;
}
