package org.poma.scanning.mr;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.poma.scanning.rfile.RFileRecordReader;

import java.io.IOException;

public class RFileInputFormat extends FileInputFormat<Key, Value> {

    @Override
    public RecordReader<Key,Value> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RFileRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

}