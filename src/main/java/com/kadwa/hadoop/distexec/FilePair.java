/*
 * Created by Neville Kadwa.
 */
package com.kadwa.hadoop.distexec;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An input/output pair of filenames.
 */
public class FilePair implements Writable {
    FileStatus input = new FileStatus();
    String output;

    public FilePair() {
    }

    public FilePair(FileStatus input, String output) {
        this.input = input;
        this.output = output;
    }

    public void readFields(DataInput in) throws IOException {
        input.readFields(in);
        output = Text.readString(in);
    }

    public void write(DataOutput out) throws IOException {
        input.write(out);
        Text.writeString(out, output);
    }

    public String toString() {
        return input + " : " + output;
    }
}
