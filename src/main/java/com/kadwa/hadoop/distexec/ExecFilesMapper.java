/*
 * Created by Neville Kadwa.
 */
package com.kadwa.hadoop.distexec;

import com.kadwa.hadoop.DistExec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ExecFilesMapper implements Mapper<LongWritable, FilePair, WritableComparable<?>, Text> {

    public static final Log LOG = LogFactory.getLog(ExecFilesMapper.class);

    static enum Counter {EXECUTED, FAIL, BYTESEXECUTED, BYTESWRITTEN}

    // config
    private FileSystem destFileSys = null;
    private boolean redirectErrorToOut;
    private Path destPath = null;
    private String execCmd = null;
    private JobConf job;
    private int sizeBuf = 128 * 1024;

    // stats
    private int failcount = 0;
    private int skipcount = 0;
    private int copycount = 0;

    private void updateStatus(Reporter reporter) {
        reporter.setStatus("Processing and stuff");
    }

    class LazyCreateOutputStream extends OutputStream {
        private Path path;
        private Reporter reporter;
        private OutputStream out;

        LazyCreateOutputStream(Path path, Reporter reporter) {
            this.path = path;
            this.reporter = reporter;
            this.out = null;
        }

        public void write(int b) throws IOException {
            if (out == null) {
                if (destFileSys.exists(path)) {
                    destFileSys.delete(path, false);
                }
                out = destFileSys.create(path, true, sizeBuf, reporter);
            }
            out.write(b);
        }

        @Override
        public void flush() throws IOException {
            if (out != null) {
                out.flush();
            }
        }

        @Override
        public void close() throws IOException {
            if (out != null) {
                out.close();
            }
        }
    }

    private void execution(FileStatus srcstat, String relativedst,
                           OutputCollector<WritableComparable<?>, Text> outc, Reporter reporter)
            throws IOException {
        Path absdst = new Path(destPath, relativedst);
        Path abserr = new Path(destPath, relativedst+".stderr");
        int totfiles = job.getInt(DistExec.SRC_COUNT_LABEL, -1);
        assert totfiles >= 0 : "Invalid file count " + totfiles;

        // if a directory, ensure created even if empty
        if (srcstat.isDir()) {
            if (destFileSys.exists(absdst)) {
                if (!destFileSys.getFileStatus(absdst).isDir()) {
                    throw new IOException("Failed to mkdirs: " + absdst+" is a file.");
                }
            }
            else if (!destFileSys.mkdirs(absdst)) {
                throw new IOException("Failed to mkdirs " + absdst);
            }
            // TODO: when modification times can be set, directories should be
            // emitted to reducers so they might be preserved. Also, mkdirs does
            // not currently return an error when the directory already exists;
            // if this changes, all directory work might as well be done in reduce
            return;
        }

        Path tmpfile = new Path(job.get(DistExec.TMP_DIR_LABEL), relativedst);
        Path errfile = new Path(job.get(DistExec.TMP_DIR_LABEL), relativedst+".stderr");
        InputStream in = null;
        OutputStream out = null;
        OutputStream err = null;
        try {
            // open src file
            in = srcstat.getPath().getFileSystem(job).open(srcstat.getPath());
            reporter.incrCounter(Counter.BYTESEXECUTED, srcstat.getLen());

            // open tmp file
            out = new LazyCreateOutputStream(tmpfile, reporter);
            err = new LazyCreateOutputStream(errfile, reporter);

            Executor executor = new Executor(this.execCmd, in, out, err);
            executor.execute();
            reporter.incrCounter(Counter.BYTESWRITTEN, executor.getBytesOutputCount());
        } catch(InterruptedException iex) {
            throw new IOException("Process was interrupted", iex);
        } finally {
            DistExec.checkAndClose(in);
            DistExec.checkAndClose(out);
        }

        if (totfiles == 1) {
            // Running a single file; use dst path provided by user as destination
            // rather than destination directory, if a file
            Path dstparent = absdst.getParent();
            if (!(destFileSys.exists(dstparent) &&
                    destFileSys.getFileStatus(dstparent).isDir())) {
                absdst = dstparent;
            }
        }
        if (destFileSys.exists(absdst) &&
                destFileSys.getFileStatus(absdst).isDir()) {
            throw new IOException(absdst + " is a directory");
        }
        if (!destFileSys.mkdirs(absdst.getParent())) {
            throw new IOException("Failed to create parent dir: " + absdst.getParent());
        }
        rename(tmpfile, absdst);
        rename(errfile, abserr);

        // report at least once for each file
        ++copycount;
        reporter.incrCounter(Counter.EXECUTED, 1);
        updateStatus(reporter);
    }

    /** rename tmp to dst, delete dst if already exists */
    private void rename(Path tmp, Path dst) throws IOException {
        try {
            if (destFileSys.exists(dst)) {
                destFileSys.delete(dst, true);
            }
            if (destFileSys.exists(tmp) && !destFileSys.rename(tmp, dst)) {
                throw new IOException();
            }
        }
        catch(IOException cause) {
            throw (IOException)new IOException("Fail to rename tmp file (=" + tmp
                    + ") to destination file (=" + dst + ")").initCause(cause);
        }
    }


    public void map(LongWritable key, FilePair value, OutputCollector<WritableComparable<?>, Text> out,
                    Reporter reporter) throws IOException {

        final FileStatus srcstat = value.input;
        final String relativedst = value.output;
        try {
            execution(srcstat, relativedst, out, reporter);
        } catch (IOException e) {
            ++failcount;
            reporter.incrCounter(Counter.FAIL, 1);
            updateStatus(reporter);
            final String sfailure = "FAIL " + relativedst + " : " +
                    StringUtils.stringifyException(e);
            out.collect(null, new Text(sfailure));
            LOG.info(sfailure);
            try {
                for (int i = 0; i < 3; ++i) {
                    try {
                        final Path tmp = new Path(job.get(DistExec.TMP_DIR_LABEL), relativedst);
                        if (destFileSys.delete(tmp, true))
                            break;
                    } catch (Throwable ex) {
                        // ignore, we are just cleaning up
                        LOG.debug("Ignoring cleanup exception", ex);
                    }
                    // update status, so we don't get timed out
                    updateStatus(reporter);
                    Thread.sleep(3 * 1000);
                }
            } catch (InterruptedException inte) {
                throw (IOException)new IOException().initCause(inte);
            }
        } finally {
            updateStatus(reporter);
        }
    }

    static String bytesString(long b) {
        return b + " bytes (" + StringUtils.humanReadableInt(b) + ")";
    }

    public void close() throws IOException {

    }

    /** Mapper configuration.
     * Extracts source and destination file system, as well as
     * top-level paths on source and destination directories.
     * Gets the named file systems, to be used later in map.
     */
    public void configure(JobConf job) {
        destPath = new Path(job.get(DistExec.DST_DIR_LABEL, "/"));
        try {
            destFileSys = destPath.getFileSystem(job);
        } catch (IOException ex) {
            throw new RuntimeException("Unable to get the named file system.", ex);
        }
        execCmd = job.get(DistExec.EXEC_CMD_LABEL);
        sizeBuf = job.getInt("copy.buf.size", 128 * 1024);
        redirectErrorToOut = job.getBoolean(DistExec.Options.REDIRECT_ERROR_TO_OUT.propertyname, false);
        this.job = job;
    }

}
