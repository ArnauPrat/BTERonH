package ldbc.snb.bteronhplus.structures;

import java.io.IOException;
import java.io.OutputStreamWriter;

public class FileEdgeWriter implements EdgeWriter {
    
    private OutputStreamWriter writer = null;
    
    public FileEdgeWriter(OutputStreamWriter writer) {
        this.writer = writer;
    }
    
    @Override
    public void write(long tail, long head) throws IOException {
        writer.write(tail + "\t" + head + "\n");
    }
    
    @Override
    public void close() throws IOException {
        writer.close();
    }
}
