package com.kadwa.hadoop.distexec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

/**
 * Created by Neville Kadwa.
 */
public class SingleExecution {

    private final static int BUFFER_SIZE = 128 * 1024;
    public static final Log LOG = LogFactory.getLog(SingleExecution.class);

    private Process process;
    SingleTransferThread inPrinter;
    SingleTransferThread outPrinter;
    SingleTransferThread errPrinter;

    private SingleExecution(ProcessBuilder builder, InputStream inputStream,
                            OutputStream outputStream, OutputStream errorStream) throws IOException {
        process = builder.start();
        if (outputStream != null) {
            InputStream processOut = new BufferedInputStream(process.getInputStream(), BUFFER_SIZE); // STDOUT
            outPrinter = new SingleTransferThread(processOut, outputStream, false, "STDOUT");
            outPrinter.start();
        }
        if (errorStream != null) {
            InputStream processErr = new BufferedInputStream(process.getErrorStream(), BUFFER_SIZE); // STDERR
            errPrinter = new SingleTransferThread(processErr, errorStream, false, "STDERR");
            errPrinter.start();
        }
        if (inputStream != null) {
            OutputStream processIn = new BufferedOutputStream(process.getOutputStream(), BUFFER_SIZE); // STDIN
            inPrinter = new SingleTransferThread(inputStream, processIn, true, "STDIN");
            inPrinter.start();
        }
    }

    public long getBytesOutputCount() {
        return outPrinter.getBytesXfered();
    }

    public int waitFor() throws InterruptedException {
        int exitVal = process.waitFor();
        if (outPrinter != null) {
            outPrinter.join(10000);
        }
        if (errPrinter != null) {
            errPrinter.join(10000);
        }
        if (inPrinter != null) {
            inPrinter.close();
        }
        return exitVal;
    }

    public static SingleExecution execute(ProcessBuilder builder, InputStream inputStream,
                                         OutputStream outputStream, OutputStream errorStream) throws IOException {
        return new SingleExecution(builder, inputStream, outputStream, errorStream);
    }


    private static class SingleTransferThread extends Thread {
        private InputStream in;
        private OutputStream out;
        private boolean transferClose;
        private long bytesXfered;
        private String logTag;

        public SingleTransferThread(InputStream in, OutputStream out, boolean transferClose, String logTag) {
            this.in = in;
            this.out = out;
            this.transferClose = transferClose;
            this.logTag = logTag;
        }

        public void run() {
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
                    try { out.close(); } catch (IOException iex) { /*ignored*/ }
                }
            }
            LOG.debug("Returning SingleTransferThread: " + logTag + " (" + bytesXfered + ")");
        }

        public long getBytesXfered() {
            return bytesXfered;
        }

        public void close() {
            try {
                LOG.debug("Closing SingleTransferThread: " + logTag + " (" + bytesXfered + ")");
                this.in.close();
            } catch (Exception e) {
            }
        }
    }

}
