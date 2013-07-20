/*
You should have received a copy of the GNU General Public License
along with GNU Classpath; see the file COPYING.  If not, write to the
Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
02110-1301 USA.

Linking this library statically or dynamically with other modules is
making a combined work based on this library.  Thus, the terms and
conditions of the GNU General Public License cover the whole
combination.

As a special exception, the copyright holders of this library give you
permission to link this library with independent modules to produce an
executable, regardless of the license terms of these independent
modules, and to copy and distribute the resulting executable under
terms of your choice, provided that you also meet, for each linked
independent module, the terms and conditions of the license of that
module.  An independent module is a module which is not derived from
or based on this library.  If you modify this library, you may extend
this exception to your version of the library, but you are not
obligated to do so.  If you do not wish to do so, delete this
exception statement from your version.

The delivery of Props are required for use of this software or any
derivative work for commercial or non-commercial purposes. This requirement
does not go into effect during an initial evaluation period. Once the
software is planned to be used the following Props must be delivered:
1) Follow the author on twitter: @kadwanev
2) Star the project on github: https://github.com/kadwanev/hadoop-distexec
Props must be delivered by any engineer introducing or maintaining the software's use.
A best attempt must be made to deliver props from an organization, if possible.

*/

package com.kadwa.hadoop;

import com.kadwa.hadoop.distexec.CommandLineUtil;
import com.kadwa.hadoop.distexec.Executor;
import com.kadwa.hadoop.distexec.SingleExecution;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.*;

/**
 * Created by Neville Kadwa.
 */
public class DistExec implements Tool {
    public static final Log LOG = LogFactory.getLog(DistExec.class);

    private static final String NAME = "distexec";

    private static final String usage = NAME
            + " [OPTIONS] <srcurl>* <desturl> <exec cmd>" +
            "\n\nOPTIONS:" +
            "\n-singleOut             Combine all output to single" +
            "\n-m <num_maps>          Maximum number of simultaneous executions" +
            "\n";

    private static final long BYTES_PER_MAP =  256 * 1024 * 1024;
    private static final int MAX_MAPS_PER_NODE = 20;
    private static final int SYNC_FILE_MAX = 10;

    static enum Counter {EXECUTED, FAIL, BYTESEXECUTED, BYTESWRITTEN}

    static enum Options {
        SINGLE_OUT("-singleOut", NAME + ".single.out");

        final String cmd, propertyname;

        private Options(String cmd, String propertyname) {
            this.cmd = cmd;
            this.propertyname = propertyname;
        }
    }

    static final String EXEC_CMD_LABEL = NAME + ".exec.cmd";
    static final String TMP_DIR_LABEL = NAME + ".tmp.dir";
    static final String DST_DIR_LABEL = NAME + ".dest.path";
    static final String JOB_DIR_LABEL = NAME + ".job.dir";
    static final String MAX_MAPS_LABEL = NAME + ".max.map.tasks";
    static final String SRC_LIST_LABEL = NAME + ".src.list";
    static final String SRC_COUNT_LABEL = NAME + ".src.count";
    static final String TOTAL_SIZE_LABEL = NAME + ".total.size";
    static final String DST_DIR_LIST_LABEL = NAME + ".dst.dir.list";

    private JobConf conf;

    public void setConf(Configuration conf) {
        if (conf instanceof JobConf) {
            this.conf = (JobConf) conf;
        } else {
            this.conf = new JobConf(conf);
        }
    }

    public Configuration getConf() {
        return conf;
    }

    public DistExec(Configuration conf) {
        setConf(conf);
    }

    /**
     * An input/output pair of filenames.
     */
    static class FilePair implements Writable {
        FileStatus input = new FileStatus();
        String output;

        FilePair() {
        }

        FilePair(FileStatus input, String output) {
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

    /**
     * InputFormat of a distexec job responsible for generating splits of the src
     * file list.
     */
    static class ExecInputFormat implements InputFormat<Text, Text> {

        /**
         * Produce splits such that each is no greater than the quotient of the
         * total size and the number of splits requested.
         *
         * @param job       The handle to the JobConf object
         * @param numSplits Number of splits requested
         */
        public InputSplit[] getSplits(JobConf job, int numSplits)
                throws IOException {
            int cnfiles = job.getInt(SRC_COUNT_LABEL, -1);
            long cbsize = job.getLong(TOTAL_SIZE_LABEL, -1);
            String srcfilelist = job.get(SRC_LIST_LABEL, "");
            if (cnfiles < 0 || cbsize < 0 || "".equals(srcfilelist)) {
                throw new RuntimeException("Invalid metadata: #files(" + cnfiles +
                        ") total_size(" + cbsize + ") listuri(" +
                        srcfilelist + ")");
            }
            Path src = new Path(srcfilelist);
            FileSystem fs = src.getFileSystem(job);
            FileStatus srcst = fs.getFileStatus(src);

            ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
            LongWritable key = new LongWritable();
            FilePair value = new FilePair();
            final long targetsize = cbsize / numSplits;
            long pos = 0L;
            long last = 0L;
            long acc = 0L;
            long cbrem = srcst.getLen();
            SequenceFile.Reader sl = null;
            try {
                sl = new SequenceFile.Reader(fs, src, job);
                for (; sl.next(key, value); last = sl.getPosition()) {
                    // if adding this split would put this split past the target size,
                    // cut the last split and put this next file in the next split.
                    if (acc + key.get() > targetsize && acc != 0) {
                        long splitsize = last - pos;
                        splits.add(new FileSplit(src, pos, splitsize, (String[]) null));
                        cbrem -= splitsize;
                        pos = last;
                        acc = 0L;
                    }
                    acc += key.get();
                }
            } finally {
                checkAndClose(sl);
            }
            if (cbrem != 0) {
                splits.add(new FileSplit(src, pos, cbrem, (String[]) null));
            }

            return splits.toArray(new FileSplit[splits.size()]);
        }

        /**
         * Returns a reader for this split of the src file list.
         */
        public RecordReader<Text, Text> getRecordReader(InputSplit split,
                                                        JobConf job, Reporter reporter) throws IOException {
            return new SequenceFileRecordReader<Text, Text>(job, (FileSplit) split);
        }
    }

    /**
     * ExecFilesMapper: The mapper for copying files between FileSystems.
     */
    static class ExecFilesMapper implements Mapper<LongWritable, FilePair, WritableComparable<?>, Text> {
        // config
        private FileSystem destFileSys = null;
        private boolean ignoreReadFailures;
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

        private FSDataOutputStream create(Path f, Reporter reporter,
                                          FileStatus srcstat) throws IOException {
            if (destFileSys.exists(f)) {
                destFileSys.delete(f, false);
            }
            return destFileSys.create(f, true, sizeBuf, reporter);
        }

        private void execution(FileStatus srcstat, Path relativedst,
                          OutputCollector<WritableComparable<?>, Text> outc, Reporter reporter)
                throws IOException {
            Path absdst = new Path(destPath, relativedst);
            int totfiles = job.getInt(SRC_COUNT_LABEL, -1);
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

            Path tmpfile = new Path(job.get(TMP_DIR_LABEL), relativedst);
            FSDataInputStream in = null;
            FSDataOutputStream out = null;
            try {
                // open src file
                in = srcstat.getPath().getFileSystem(job).open(srcstat.getPath());
                reporter.incrCounter(Counter.BYTESEXECUTED, srcstat.getLen());
                // open tmp file
                out = create(tmpfile, reporter, srcstat);

                Executor executor = new Executor(this.execCmd, in, out);
                executor.execute();
                reporter.incrCounter(Counter.BYTESWRITTEN, executor.getBytesOutputCount());
            } catch(InterruptedException iex) {
                throw new IOException("Process was interrupted", iex);
            } finally {
                checkAndClose(in);
                checkAndClose(out);
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
                if (!destFileSys.rename(tmp, dst)) {
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
            final Path relativedst = new Path(value.output);
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
                            final Path tmp = new Path(job.get(TMP_DIR_LABEL), relativedst);
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
            destPath = new Path(job.get(DST_DIR_LABEL, "/"));
            try {
                destFileSys = destPath.getFileSystem(job);
            } catch (IOException ex) {
                throw new RuntimeException("Unable to get the named file system.", ex);
            }
            execCmd = job.get(EXEC_CMD_LABEL);
            sizeBuf = job.getInt("copy.buf.size", 128 * 1024);
            this.job = job;
        }
    }

    private static List<Path> fetchFileList(Configuration conf, Path srcList)
            throws IOException {
        List<Path> result = new ArrayList<Path>();
        FileSystem fs = srcList.getFileSystem(conf);
        BufferedReader input = null;
        try {
            input = new BufferedReader(new InputStreamReader(fs.open(srcList)));
            String line = input.readLine();
            while (line != null) {
                result.add(new Path(line));
                line = input.readLine();
            }
        } finally {
            checkAndClose(input);
        }
        return result;
    }

    /**
     * Sanity check for srcPath
     */
    private static void checkSrcPath(JobConf jobConf, List<Path> srcPaths)
            throws IOException {
        List<IOException> rslt = new ArrayList<IOException>();

        Path[] ps = new Path[srcPaths.size()];
        ps = srcPaths.toArray(ps);
        TokenCache.obtainTokensForNamenodes(jobConf.getCredentials(), ps, jobConf);

        for (Path p : srcPaths) {
            FileSystem fs = p.getFileSystem(jobConf);
            if (!fs.exists(p)) {
                rslt.add(new IOException("Input source " + p + " does not exist."));
            }
        }
        if (!rslt.isEmpty()) {
            throw new InvalidInputException(rslt);
        }
    }

    static private class Arguments {
        final List<Path> srcs;
        final Path dst;
        final String execCmd;
        final Path log;
        final EnumSet<Options> flags;

        /**
         * Arguments for distexec
         *
         * @param srcs                List of source paths
         * @param dst                 Destination path
         * @param execCmd    Execution command
         * @param log                 Log output directory
         * @param flags               Command-line flags
         */
        Arguments(List<Path> srcs, Path dst, String execCmd, Path log, EnumSet<Options> flags) {
            this.srcs = srcs;
            this.dst = dst;
            this.execCmd = execCmd;
            this.log = log;
            this.flags = flags;

            if (LOG.isTraceEnabled()) {
                LOG.trace("this = " + this);
            }
        }

        static Arguments valueOf(String[] args, Configuration conf) throws IOException {
            List<Path> srcs = new ArrayList<Path>();
            Path dst = null;
            Path log = null;
            EnumSet<Options> flags = EnumSet.noneOf(Options.class);
            String executionCmd = null;

            for (int idx = 0; idx < args.length; idx++) {
                Options[] opt = Options.values();
                int i = 0;
                for (; i < opt.length && !args[idx].startsWith(opt[i].cmd); i++) ;

                if (i < opt.length) {
                    flags.add(opt[i]);
                } else if ("-m".equals(args[idx])) {
                    if (++idx == args.length) {
                        throw new IllegalArgumentException("num_maps not specified in -m");
                    }
                    try {
                        conf.setInt(MAX_MAPS_LABEL, Integer.valueOf(args[idx]));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid argument to -m: " +
                                args[idx]);
                    }
                } else if ('-' == args[idx].codePointAt(0)) {
                    throw new IllegalArgumentException("Invalid switch " + args[idx]);
                } else if (idx == args.length - 2) {
                    dst = new Path(args[idx]);
                } else if (idx == args.length - 1) {
                    executionCmd = args[idx];
                } else {
                    srcs.add(new Path(args[idx]));
                }
            }
            // mandatory command-line parameters
            if (srcs.isEmpty() || dst == null || executionCmd == null) {
                throw new IllegalArgumentException("Missing mandatory arguments: [src, dst path, exec cmd]");
            }

            return new Arguments(srcs, dst, executionCmd, log, flags);
        }

        /**
         * {@inheritDoc}
         */
        public String toString() {
            return getClass().getName() + "{"
                    + "\n  srcs = " + srcs
                    + "\n  dst = " + dst
                    + "\n  execCmd = " + execCmd
                    + "\n  log = " + log
                    + "\n  flags = " + flags
                    + "\n}";
        }
    }


    /**
     * Driver to exec srcPath to destPath depending on required protocol.
     *
     * @param args arguments
     */
    static void execution(final Configuration conf, final Arguments args
    ) throws IOException {
        LOG.info("srcPaths=" + args.srcs);
        LOG.info("destPath=" + args.dst);
        LOG.info("execCmd=" + args.execCmd);

        JobConf job = createJobConf(conf);

        checkSrcPath(job, args.srcs);

        //Initialize the mapper
        try {
            if (setup(conf, job, args)) {
                JobClient.runJob(job);
            }
        } finally {
            //delete tmp
            fullyDelete(job.get(TMP_DIR_LABEL), job);
            //delete jobDirectory
            fullyDelete(job.get(JOB_DIR_LABEL), job);
        }
    }


    /**
     * This is the main driver for recursively copying directories
     * across file systems. It takes at least two cmdline parameters. A source
     * URL and a destination URL. It then essentially does an "ls -lR" on the
     * source URL, and writes the output in a round-robin manner to all the map
     * input files. The mapper actually copies the files allotted to it. The
     * reduce is empty.
     */
    public int run(String[] args) {
        try {
            execution(conf, Arguments.valueOf(args, conf));
            return 0;
        } catch (IllegalArgumentException e) {
            System.err.println(StringUtils.stringifyException(e) + "\n" + usage);
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        } catch (DuplicationException e) {
            System.err.println(StringUtils.stringifyException(e));
            return DuplicationException.ERROR_CODE;
        } catch (RemoteException e) {
            final IOException unwrapped = e.unwrapRemoteException(
                    FileNotFoundException.class,
                    AccessControlException.class,
                    QuotaExceededException.class);
            System.err.println(StringUtils.stringifyException(unwrapped));
            return -3;
        } catch (Exception e) {
            System.err.println("With failures, global counters are inaccurate; " +
                    "consider running with -i");
            System.err.println("Execution failed: " + StringUtils.stringifyException(e));
            return -999;
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf job = new JobConf(DistExec.class);
        DistExec distexec = new DistExec(job);
        int res = ToolRunner.run(distexec, args);
        System.exit(res);
    }

    /**
     * Make a path relative with respect to a root path.
     * absPath is always assumed to descend from root.
     * Otherwise returned path is null.
     */
    static String makeRelative(Path root, Path absPath) {
        if (!absPath.isAbsolute()) {
            throw new IllegalArgumentException("!absPath.isAbsolute(), absPath="
                    + absPath);
        }
        String p = absPath.toUri().getPath();

        StringTokenizer pathTokens = new StringTokenizer(p, "/");
        for (StringTokenizer rootTokens = new StringTokenizer(
                root.toUri().getPath(), "/"); rootTokens.hasMoreTokens(); ) {
            if (!rootTokens.nextToken().equals(pathTokens.nextToken())) {
                return null;
            }
        }
        StringBuilder sb = new StringBuilder();
        for (; pathTokens.hasMoreTokens(); ) {
            sb.append(pathTokens.nextToken());
            if (pathTokens.hasMoreTokens()) {
                sb.append(Path.SEPARATOR);
            }
        }
        return sb.length() == 0 ? "." : sb.toString();
    }

    /**
     * Calculate how many maps to run. Number of maps is equal to the number of files.
     *
     * @param fileCount Count of total files for job
     * @param job        The job to configure
     * @return Count of maps to run.
     */
    private static void setMapCount(long fileCount, JobConf job)
            throws IOException {
        int numMaps = (int) fileCount;
        numMaps = Math.min(numMaps,
                job.getInt(MAX_MAPS_LABEL, MAX_MAPS_PER_NODE *
                        new JobClient(job).getClusterStatus().getTaskTrackers()));
        job.setNumMapTasks(Math.max(numMaps, 1));
    }

    /**
     * Fully delete dir
     */
    static void fullyDelete(String dir, Configuration conf) throws IOException {
        if (dir != null) {
            Path tmp = new Path(dir);
            tmp.getFileSystem(conf).delete(tmp, true);
        }
    }

    //Job configuration
    private static JobConf createJobConf(Configuration conf) {
        JobConf jobconf = new JobConf(conf, DistExec.class);
        jobconf.setJobName(NAME);

        // turn off speculative execution, because DFS doesn't handle
        // multiple writers to the same file.
        jobconf.setMapSpeculativeExecution(false);

        jobconf.setInputFormat(ExecInputFormat.class);
        jobconf.setOutputKeyClass(Text.class);
        jobconf.setOutputValueClass(Text.class);

        jobconf.setMapperClass(ExecFilesMapper.class);
        jobconf.setNumReduceTasks(0);
        // TODO implement singleOut by setting single reducer and prepending file name to output
        return jobconf;
    }

    private static final Random RANDOM = new Random();

    public static String getRandomId() {
        return Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE), 36);
    }

    /**
     * Initialize ExecFilesMapper specific job-configuration.
     *
     * @param conf    : The dfs/mapred configuration.
     * @param jobConf : The handle to the jobConf object to be initialized.
     * @param args    Arguments
     * @return true if it is necessary to launch a job.
     */
    private static boolean setup(Configuration conf, JobConf jobConf,
                                 final Arguments args)
            throws IOException {
        jobConf.set(DST_DIR_LABEL, args.dst.toUri().toString());
        jobConf.set(EXEC_CMD_LABEL, args.execCmd);

        //set boolean values
        jobConf.setBoolean(Options.SINGLE_OUT.propertyname,
                args.flags.contains(Options.SINGLE_OUT));

        final String randomId = getRandomId();
        JobClient jClient = new JobClient(jobConf);
        Path stagingArea;
        try {
            stagingArea = JobSubmissionFiles.getStagingDir(jClient, conf);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        Path jobDirectory = new Path(stagingArea + NAME + "_" + randomId);
        FsPermission mapredSysPerms = new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
        FileSystem.mkdirs(FileSystem.get(jobDirectory.toUri(), conf), jobDirectory, mapredSysPerms);
        jobConf.set(JOB_DIR_LABEL, jobDirectory.toString());

        FileSystem dstfs = args.dst.getFileSystem(conf);

        // get tokens for all the required FileSystems..
        TokenCache.obtainTokensForNamenodes(jobConf.getCredentials(),
                new Path[]{args.dst}, conf);

        boolean dstExists = dstfs.exists(args.dst);
        boolean dstIsDir = false;
        if (dstExists) {
            dstIsDir = dstfs.getFileStatus(args.dst).isDir();
        }

        // default logPath
        Path logPath = args.log;
        if (logPath == null) {
            String filename = "_" + NAME +  "_logs_" + randomId;
            if (!dstExists || !dstIsDir) {
                Path parent = args.dst.getParent();
                if (!dstfs.exists(parent)) {
                    dstfs.mkdirs(parent);
                }
                logPath = new Path(parent, filename);
            } else {
                logPath = new Path(args.dst, filename);
            }
        }
        FileOutputFormat.setOutputPath(jobConf, logPath);

        // create src list, dst list
        FileSystem jobfs = jobDirectory.getFileSystem(jobConf);

        Path srcfilelist = new Path(jobDirectory, "_" + NAME + "_src_files");
        jobConf.set(SRC_LIST_LABEL, srcfilelist.toString());
        SequenceFile.Writer src_writer = SequenceFile.createWriter(jobfs, jobConf,
                srcfilelist, LongWritable.class, FilePair.class,
                SequenceFile.CompressionType.NONE);

        Path dstfilelist = new Path(jobDirectory, "_" + NAME + "_dst_files");
        SequenceFile.Writer dst_writer = SequenceFile.createWriter(jobfs, jobConf,
                dstfilelist, Text.class, Text.class,
                SequenceFile.CompressionType.NONE);

        Path dstdirlist = new Path(jobDirectory, "_" + NAME + "_dst_dirs");
        jobConf.set(DST_DIR_LIST_LABEL, dstdirlist.toString());
        SequenceFile.Writer dir_writer = SequenceFile.createWriter(jobfs, jobConf,
                dstdirlist, Text.class, FilePair.class,
                SequenceFile.CompressionType.NONE);

        // handle the case where the destination directory doesn't exist
        // and we've only a single src directory.
        final boolean special = (args.srcs.size() == 1 && !dstExists);
        int srcCount = 0, cnsyncf = 0, dirsyn = 0;
        long fileCount = 0L, byteCount = 0L, cbsyncs = 0L;
        try {
            for (Iterator<Path> srcItr = args.srcs.iterator(); srcItr.hasNext(); ) {
                final Path src = srcItr.next();
                FileSystem srcfs = src.getFileSystem(conf);
                FileStatus srcfilestat = srcfs.getFileStatus(src);
                Path root = special && srcfilestat.isDir() ? src : src.getParent();
                if (srcfilestat.isDir()) {
                    ++srcCount;
                }

                Stack<FileStatus> pathstack = new Stack<FileStatus>();
                for (pathstack.push(srcfilestat); !pathstack.empty(); ) {
                    FileStatus cur = pathstack.pop();
                    FileStatus[] children = srcfs.listStatus(cur.getPath());
                    for (int i = 0; i < children.length; i++) {
                        boolean skipfile = false;
                        final FileStatus child = children[i];
                        final String dst = makeRelative(root, child.getPath());
                        ++srcCount;

                        if (child.isDir()) {
                            pathstack.push(child);
                        } else {

                            if (!skipfile) {
                                ++fileCount;
                                byteCount += child.getLen();

                                if (LOG.isTraceEnabled()) {
                                    LOG.trace("adding file " + child.getPath());
                                }

                                ++cnsyncf;
                                cbsyncs += child.getLen();
                                if (cnsyncf > SYNC_FILE_MAX || cbsyncs > BYTES_PER_MAP) {
                                    src_writer.sync();
                                    dst_writer.sync();
                                    cnsyncf = 0;
                                    cbsyncs = 0L;
                                }
                            }
                        }

                        if (!skipfile) {
                            src_writer.append(new LongWritable(child.isDir() ? 0 : child.getLen()),
                                    new FilePair(child, dst));
                        }

                        dst_writer.append(new Text(dst),
                                new Text(child.getPath().toString()));
                    }

                    if (cur.isDir()) {
                        String dst = makeRelative(root, cur.getPath());
                        dir_writer.append(new Text(dst), new FilePair(cur, dst));
                        if (++dirsyn > SYNC_FILE_MAX) {
                            dirsyn = 0;
                            dir_writer.sync();
                        }
                    }
                }
            }
        } finally {
            checkAndClose(src_writer);
            checkAndClose(dst_writer);
            checkAndClose(dir_writer);
        }

        FileStatus dststatus = null;
        try {
            dststatus = dstfs.getFileStatus(args.dst);
        } catch (FileNotFoundException fnfe) {
            LOG.info(args.dst + " does not exist.");
        }

        // create dest path dir if copying > 1 file
        if (dststatus == null) {
            if (srcCount > 1 && !dstfs.mkdirs(args.dst)) {
                throw new IOException("Failed to create" + args.dst);
            }
        }

        final Path sorted = new Path(jobDirectory, "_" + NAME + "_sorted");
        checkDuplication(jobfs, dstfilelist, sorted, conf);

        Path tmpDir = new Path(
                (dstExists && !dstIsDir) || (!dstExists && srcCount == 1) ?
                        args.dst.getParent() : args.dst, "_" + NAME + "_tmp_" + randomId);
        jobConf.set(TMP_DIR_LABEL, tmpDir.toUri().toString());
        LOG.info("sourcePathsCount=" + srcCount);
        LOG.info("filesToExecCount=" + fileCount);
        LOG.info("bytesToExecCount=" + StringUtils.humanReadableInt(byteCount));
        jobConf.setInt(SRC_COUNT_LABEL, srcCount);
        jobConf.setLong(TOTAL_SIZE_LABEL, byteCount);
        setMapCount(fileCount, jobConf);
        return fileCount > 0;
    }

    /** Check whether the file list have duplication. */
    static private void checkDuplication(FileSystem fs, Path file, Path sorted,
                                         Configuration conf) throws IOException {
        SequenceFile.Reader in = null;
        try {
            SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs,
                    new Text.Comparator(), Text.class, Text.class, conf);
            sorter.sort(file, sorted);
            in = new SequenceFile.Reader(fs, sorted, conf);

            Text prevdst = null, curdst = new Text();
            Text prevsrc = null, cursrc = new Text();
            for(; in.next(curdst, cursrc); ) {
                if (prevdst != null && curdst.equals(prevdst)) {
                    throw new DuplicationException(
                            "Invalid input, there are duplicated files in the sources: "
                                    + prevsrc + ", " + cursrc);
                }
                prevdst = curdst;
                curdst = new Text();
                prevsrc = cursrc;
                cursrc = new Text();
            }
        }
        finally {
            checkAndClose(in);
        }
    }

    static boolean checkAndClose(java.io.Closeable io) {
        if (io != null) {
            try {
                io.close();
            } catch (IOException ioe) {
                LOG.warn(StringUtils.stringifyException(ioe));
                return false;
            }
        }
        return true;
    }

    /** An exception class for duplicated source files. */
    public static class DuplicationException extends IOException {
        private static final long serialVersionUID = 1L;
        /** Error code for this exception */
        public static final int ERROR_CODE = -2;
        DuplicationException(String message) {super(message);}
    }

}
