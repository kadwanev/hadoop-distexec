/*
 * Created by Neville Kadwa.
 */
package com.kadwa.hadoop.distexec;

public interface Executor {

    public int waitFor() throws InterruptedException;
    public long getBytesOutputCount();

}
