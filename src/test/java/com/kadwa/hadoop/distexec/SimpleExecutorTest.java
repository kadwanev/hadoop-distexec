/*
 * Created by Neville Kadwa.
 */
package com.kadwa.hadoop.distexec;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class SimpleExecutorTest {


    @Test
    public void basicCat() throws Exception {

        String testString = "Hello this is my string";

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errorStream = new ByteArrayOutputStream();

        ProcessBuilder builder = new ProcessBuilder("cat");
//        builder.directory(new File("."));
        SingleExecution execution = SingleExecution.execute(builder, new ByteArrayInputStream(testString.getBytes()), outputStream, errorStream);

        execution.waitFor();

        assertEquals(testString, outputStream.toString());
        assertEquals(execution.getBytesOutputCount(), testString.length());
    }
}
