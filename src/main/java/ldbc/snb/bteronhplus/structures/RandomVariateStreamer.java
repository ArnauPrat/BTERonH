package ldbc.snb.bteronhplus.structures;

import umontreal.iro.lecuyer.randvar.RandomVariateGen;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class RandomVariateStreamer implements CommunityStreamer {

    public static class RandomNode implements SuperNode {

        private int degree;
        private int id;

        RandomNode(int id, int degree) {
            this.id = id;
            this.degree = degree;
        }

        @Override
        public long getSize() {
            return 1;
        }

        @Override
        public long getInternalDegree() {
            return 0;
        }

        @Override
        public long getExternalDegree() {
            return degree;
        }

        @Override
        public int getId() {
            return id;
        }

        @Override
        public void sampleEdges(FileWriter writer, Random random, long numEdges, long offset) throws IOException {
        }

        @Override
        public long sampleNode(Random random, long offset) {
            return 0;
        }
    
    }

    RandomVariateGen randomVariateGen;
    int              nextId = 0;

    public RandomVariateStreamer(RandomVariateGen randomVariateGen) {
        this.randomVariateGen = randomVariateGen;
    }
    
    
    @Override
    public Community getModel(int id) {
        return null;
    }
    
    @Override
    public Community next() {
        return null;
        //return new RandomNode(nextId++, (int)(double)randomVariateGen.nextDouble());
    }
}
