package retailiq.fs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/** Local Windows teaching runtime. Uses existing NTFS ACLs, never emulates POSIX chmod. */
public class PortableLocalFileSystem extends RawLocalFileSystem {
    @Override
    public boolean supportsSymlinks() { return false; }

    @Override
    public FileStatus getFileLinkStatus(Path path) throws IOException {
        return getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        java.nio.file.Path local = pathToFile(path).toPath();
        if (!Files.exists(local)) throw new java.io.FileNotFoundException(path.toString());
        if (!Files.isDirectory(local)) return new FileStatus[] {getFileStatus(path)};
        ArrayList<FileStatus> result = new ArrayList<>();
        try (java.nio.file.DirectoryStream<java.nio.file.Path> children = Files.newDirectoryStream(local)) {
            for (java.nio.file.Path child : children) result.add(getFileStatus(new Path(child.toUri())));
        }
        return result.toArray(new FileStatus[0]);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        java.nio.file.Path local = pathToFile(path).toPath().toAbsolutePath().normalize();
        if (!Files.exists(local)) return false;
        if (local.getParent() == null) throw new IOException("Refusing to delete a filesystem root");
        if (recursive && Files.isDirectory(local)) {
            try (java.util.stream.Stream<java.nio.file.Path> descendants = Files.walk(local)) {
                for (java.nio.file.Path child : (Iterable<java.nio.file.Path>) descendants.sorted(Comparator.reverseOrder())::iterator)
                    Files.delete(child);
            }
        } else Files.delete(local);
        return true;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        Files.createDirectories(pathToFile(path).toPath());
        return true;
    }

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        // NTFS ACLs remain in force. Hadoop POSIX mode metadata is not applicable here.
        if (!Files.exists(pathToFile(path).toPath())) throw new java.io.FileNotFoundException(path.toString());
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        java.nio.file.Path local = pathToFile(path).toPath();
        if (!Files.exists(local)) throw new java.io.FileNotFoundException(path.toString());
        BasicFileAttributes attrs = Files.readAttributes(local, BasicFileAttributes.class);
        return new FileStatus(attrs.size(), attrs.isDirectory(), 1, 4096,
            attrs.lastModifiedTime().toMillis(), attrs.lastAccessTime().toMillis(),
            FsPermission.getDefault(), System.getProperty("user.name"), "",
            path.makeQualified(getUri(), getWorkingDirectory()));
    }

    @Override
    public boolean rename(Path source, Path destination) throws IOException {
        java.nio.file.Path from = pathToFile(source).toPath();
        java.nio.file.Path to = pathToFile(destination).toPath();
        if (!Files.exists(from)) return false;
        if (Files.isDirectory(to)) to = to.resolve(from.getFileName());
        if (Files.exists(to)) return false;
        Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
        return true;
    }
}
