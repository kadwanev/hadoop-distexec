/*
 * Created by Neville Kadwa.
 */
package com.kadwa.hadoop.distexec;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class Executor {

    private String execCmd;
    private FSDataInputStream in;
    private FSDataOutputStream out;

    private int exitVal;
    private SingleExecution executor;

    public Executor(String execCmd, FSDataInputStream in, FSDataOutputStream out) {
        this.execCmd = execCmd;
        this.in = in;
        this.out = out;
    }

    private static boolean isGrepException(String command) {
        return command.matches("grep|egrep|fgrep|zgrep|zegrep|zfgrep");
    }

    public void execute() throws IOException, InterruptedException {

        String[] commandArgs = CommandLineUtil.translateCommandline(execCmd);
        boolean grepException = isGrepException(commandArgs[0]);

        ProcessBuilder builder = new ProcessBuilder(commandArgs);

//        builder.directory(new File("."));

        executor = SingleExecution.execute(builder, in, out, System.err);

        exitVal = executor.waitFor();
        if ((grepException && ( exitVal != 0 && exitVal != 1 )) || (!grepException && exitVal != 0))
            throw new IOException("Process returned with code " + exitVal);
    }

    public int getExitVal() {
        return exitVal;
    }

    public long getBytesOutputCount() {
        return executor.getBytesOutputCount();
    }

}
