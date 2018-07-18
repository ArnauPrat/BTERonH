package ldbc.snb.bteronhplus.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;

public class FileTools {

    public static String isHDFSPath(String fileName) {
        if (fileName.startsWith("hdfs://")) {
            return fileName.substring(7);
        }
        return null;
    }

    public static String isLocalPath(String fileName) {
        if (fileName.startsWith("file://")) {
            return fileName.substring(7);
        }
        return null;
    }
    
    public static String readFile(BufferedReader reader) throws IOException {
        StringBuilder builder = new StringBuilder();
        char chars[] = new char[4096];
        int numRead = 0;
        while((numRead = reader.read(chars)) > 0) {
            if(numRead == 4096) {
                builder.append(chars);
            } else {
                builder.append(chars, 0, numRead);
            }
        }
        return builder.toString();
    }

    public static BufferedReader getFile(String fileName, Configuration conf) throws IOException {
        String realFileName;
        if ((realFileName = isHDFSPath(fileName)) != null) {
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream file = fs.open(new Path(realFileName));
            InputStreamReader reader = new InputStreamReader(file);
            return new BufferedReader(reader);
        } else if((realFileName = isLocalPath(fileName)) != null) {
            return new BufferedReader(new FileReader(realFileName));
        } else {
            throw new IOException("Invalid file URI "+fileName+". It must start with hdfs:// or file://");
        }
    }

    static public <T,S> void serializeHashMap (HashMap<T,S> map, String path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fs.create(new Path(path)));
        objectOutputStream.writeObject(map);
        objectOutputStream.close();
    }

    static public <T,S> HashMap<T,S> deserializeHashMap (String path, Configuration conf) throws
            IOException {
        FileSystem fs = FileSystem.get(conf);
        ObjectInputStream objectInputStream = new ObjectInputStream(fs.open(new Path(path)));
        try {
            HashMap<T,S> map =  (HashMap<T, S>) objectInputStream.readObject();
            objectInputStream.close();
            return map;
        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            System.exit(1);
        }
        objectInputStream.close();
        return null;
    }

    static public <T>  void serializeObjectArray( T[] array, String path, Configuration conf ) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fs.create(new Path(path)));
        objectOutputStream.writeObject(array);
        objectOutputStream.close();

    }

    static public <T>  T[] deserializeObjectArray( String path, Configuration conf ) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        ObjectInputStream objectInputStream = new ObjectInputStream(fs.open(new Path(path)));
        try {
            T array [] = (T[])objectInputStream.readObject();
            objectInputStream.close();
            return array;
        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            System.exit(1);
        }
        objectInputStream.close();
        return null;

    }

    static public <T>  void serializeObject( T object, String path, Configuration conf ) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fs.create(new Path(path)));
        objectOutputStream.writeObject(object);
        objectOutputStream.close();

    }

    static public <T>  T deserializeObject( String path, Configuration conf ) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        ObjectInputStream objectInputStream = new ObjectInputStream(fs.open(new Path(path)));
        try {
            T object = (T)objectInputStream.readObject();
            objectInputStream.close();
            return object;
        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            System.exit(1);
        }
        objectInputStream.close();
        return null;

    }
    
    public static void writeToOutputFile(String filename, int numMaps, Configuration conf) {
        try {
            FileSystem dfs = FileSystem.get(conf);
            OutputStream output = dfs.create(new Path(filename));
            for (int i = 0; i < numMaps; i++)
                output.write((new String(i + "\n").getBytes()));
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
}


}
