package com.kadwa.hadoop.distexec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

/**
 * Created by Neville Kadwa.
 */
public class SimpleExecutor extends Thread implements Executor {

    private final static int BUFFER_SIZE = 128 * 1024;
    public static final Log LOG = LogFactory.getLog(SimpleExecutor.class);

    private Process process;
    BackgroundPrinter inPrinter;
    BackgroundPrinter outPrinter;
    BackgroundPrinter errPrinter;

    private SimpleExecutor(ProcessBuilder builder, InputStream inputStream,
                           OutputStream outputStream, OutputStream errorStream) throws IOException {
        process = builder.start();
        if (outputStream != null) {
            InputStream processOut = new BufferedInputStream(process.getInputStream(), BUFFER_SIZE); // STDOUT
            outPrinter = new BackgroundPrinter(processOut, outputStream, false, "STDOUT");
            outPrinter.start();
        }
        if (errorStream != null) {
            InputStream processErr = new BufferedInputStream(process.getErrorStream(), BUFFER_SIZE); // STDERR
            errPrinter = new BackgroundPrinter(processErr, errorStream, false, "STDERR");
            errPrinter.start();
        }
        if (inputStream != null) {
            OutputStream processIn = new BufferedOutputStream(process.getOutputStream(), BUFFER_SIZE); // STDIN
            inPrinter = new BackgroundPrinter(inputStream, processIn, true, "STDIN");
            inPrinter.start();
        }
    }

    public long getBytesOutput() {
        return outPrinter.getBytesXfered();
    }

    @Override
    public int waitFor() throws InterruptedException {
        System.err.println("Initiating waitFor");
        int exitVal = process.waitFor();
        System.err.println("waitFor returned " + exitVal);
        if (outPrinter != null) {
            System.err.println("joining out");
            outPrinter.join(10000);
            System.err.println("joining out completed");
        }
        if (errPrinter != null) {
            System.err.println("joining err");
            errPrinter.join(10000);
            System.err.println("joining err completed");
        }
        if (inPrinter != null) {
            System.err.println("closing in");
            inPrinter.close();
            System.err.println("closing in completed");
        }
        return exitVal;
    }

    public static SimpleExecutor execute(ProcessBuilder builder, InputStream inputStream,
                                         OutputStream outputStream, OutputStream errorStream) throws IOException {
        return new SimpleExecutor(builder, inputStream, outputStream, errorStream);
    }


    private static class BackgroundPrinter extends Thread {
        private InputStream in;
        private OutputStream out;
        private boolean transferClose;
        private long bytesXfered;
        private String logTag;

        public BackgroundPrinter(InputStream in, OutputStream out, boolean transferClose, String logTag) {
            this.in = in;
            this.out = out;
            this.transferClose = transferClose;
            this.logTag = logTag;
        }

        public void run() {
            System.err.println("Start BackgroundPrinter: " + logTag);
            try {
                // read buffer
                byte[] buf = new byte[1024];

                // write data to target, until no more data is left to read
                int numberOfReadBytes;
                while ((numberOfReadBytes = in.read(buf)) != -1) {
                    out.write(buf, 0, numberOfReadBytes);
                    bytesXfered += numberOfReadBytes;
                }
            } catch (Exception e) {
                LOG.error(logTag, e);
            }
            finally {
                if (transferClose) {
                    LOG.debug("Transferring Close BackgroundPrinter: " + logTag);
                    try { out.close(); } catch (IOException iex) { /*ignored*/ }
                }
            }
            LOG.debug("Returning BackgroundPrinter: " + logTag + " (" + bytesXfered + ")");
        }

        public long getBytesXfered() {
            return bytesXfered;
        }

        public void close() {
            try {
                LOG.debug("Closing BackgroundPrinter: " + logTag + " (" + bytesXfered + ")");
                this.in.close();
            } catch (Exception e) {
            }
        }
    }

}
