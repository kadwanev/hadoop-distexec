= hadoop-distexec


Distributed command execution on Hadoop MapReduce.

Implemented similarly to distcp as a tool.

== Installing

Build using ant and copy hadoop-distexec.jar to $HADOOP_HOME/lib


== Optional Tool Install (patch $HADOOP_HOME/bin/hadoop):
```shell
  echo "  distcp <srcurl> <desturl> copy file or directories recursively"
+  echo "  distexec <srcurl> <desturl> <exec cmd> execute pipe command on file or directories recursively"
  echo "  archive -archiveName NAME -p <parent path> <src>* <dest> create a hadoop archive"

...

elif [ "$COMMAND" = "distcp" ] ; then
  CLASS=org.apache.hadoop.tools.DistCp
  CLASSPATH=${CLASSPATH}:${TOOL_PATH}
  HADOOP_OPTS="$HADOOP_OPTS $HADOOP_CLIENT_OPTS"
  HADOOP_HEAPSIZE_OVERRIDE="$HADOOP_CLIENT_HEAPSIZE"
+elif [ "$COMMAND" = "distexec" ] ; then
+  CLASS=com.kadwa.hadoop.DistExec
+  CLASSPATH=${CLASSPATH}:$HADOOP_HOME/lib/hadoop-distexec.jar
+  HADOOP_OPTS="$HADOOP_OPTS $HADOOP_CLIENT_OPTS"
+  HADOOP_HEAPSIZE_OVERRIDE="$HADOOP_CLIENT_HEAPSIZE"
elif [ "$COMMAND" = "daemonlog" ] ; then
  CLASS=org.apache.hadoop.log.LogLevel
  HADOOP_OPTS="$HADOOP_OPTS $HADOOP_CLIENT_OPTS"
  HADOOP_HEAPSIZE_OVERRIDE="$HADOOP_CLIENT_HEAPSIZE"
```

== Running

```shell
distexec [OPTIONS] <srcurl>* <desturl> <exec cmd>

OPTIONS:
-singleOut             Combine all output to single
-m <num_maps>          Maximum number of simultaneous executions
```

Simple copy test:
```
hadoop distexec /some/location /some/transformed cat
```

TODO list maintained in Issues

== License

Licensed under GPLv2 with linking exception. Additional Props requirements described in LICENSE file.
