package retailiq.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

/** FileContext companion used by Structured Streaming checkpoint writers. */
public class PortableLocalFs extends DelegateToFileSystem {
    public PortableLocalFs(URI uri, Configuration conf) throws IOException, URISyntaxException {
        super(uri, new PortableLocalFileSystem(), conf, "file", false);
    }
}
