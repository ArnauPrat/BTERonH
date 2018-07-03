package ldbc.snb.bteronhplus.structures;

import java.util.*;

public class BlockModel {

    final static String SEPARATOR = "\\|";
    final static float CLUSTER_THRESHOLD = 0.8f;
    final static int   MAX_BLOCKS = 2000;

    public static class ModelEntry {

        public int id;
        public double size;
        public HashMap<Integer,Double> degree = new HashMap<Integer,Double>();
        public double totalDegree = 0.0;
        public double externalDegree = 0.0;


        public ModelEntry() {
            id = 0;
            size = 0;
        }

        public ModelEntry(String entry) {
            String fields[] = entry.split(SEPARATOR);
            this.id = Integer.parseInt(fields[0]);
            this.size = Double.parseDouble(fields[1]);
            for( int i = 2; i < fields.length; ++i) {
                String cell[] = fields[i].split(":");
                int idOther = Integer.parseInt(cell[0]);
                double degree = Double.parseDouble(cell[1]);
                this.degree.put(idOther,degree);
                totalDegree+=degree;
                if(idOther != this.id) {
                    externalDegree += degree;
                }
            }
        }

    }

    Map<Integer, ModelEntry> entries = new HashMap<Integer, ModelEntry>();
    Map<Integer, List<Integer>> children = new HashMap<Integer, List<Integer>>();

    public BlockModel(String blockmodel, String childrenmodel) {
        String[] lines = blockmodel.split("\n");

        // Parsing entries
        for(int i = 0; i < lines.length; ++i) {
            ModelEntry entry = new ModelEntry(lines[i]);
            entries.put(entry.id, entry);
        }

        lines = childrenmodel.split("\n");
        for(int i = 0; i < lines.length; ++i) {
            String line[] = lines[i].split(SEPARATOR);
            int id = Integer.parseInt(line[0]);
            List<Integer> blockChildren = children.get(id);
            if(blockChildren == null) {
                blockChildren = new ArrayList<Integer>();
                children.put(id,blockChildren);
            }

            for(int j = 1; j < line.length; ++j) {
                blockChildren.add(Integer.parseInt(line[j]));
            }
        }
    }

    public static BlockModel identity() {
        return new BlockModel("0|1.0|0:1.0", "0|0");
    }

    public Map<Integer, ModelEntry> getEntries() {
        return entries;
    }

    public void setChildren(Map<Integer,List<Integer>> children) {
        this.children = children;
    }

    public int getNumBlocks() {
        return entries.size();
    }

    public Map<Integer, List<Integer>> getChildren() {
        return children;
    }
}
