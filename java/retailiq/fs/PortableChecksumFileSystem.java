package retailiq.fs;

import org.apache.hadoop.fs.LocalFileSystem;

/** Keep Hadoop's LocalFileSystem type contract for RDD text/model metadata reads. */
public class PortableChecksumFileSystem extends LocalFileSystem {
    public PortableChecksumFileSystem() {
        super(new PortableLocalFileSystem());
    }
}
