package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class GzipFileReader extends  TextFileReader {

    public GzipFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, config);
        InputStream gzipStream = new GZIPInputStream(fs.open(filePath));
        InputStreamReader decoder = new InputStreamReader(gzipStream);
        this.reader = new LineNumberReader(decoder);
    }
}
