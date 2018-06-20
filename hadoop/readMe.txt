I ran the below three commands to run my MutualFriends.java file. Make sure that /user/anirud/output folder don't exist in hdfs. Else it will give an error. The first command compiles the java file. Second command creates a jar named wc.jar. The third command runs the jar in hadoop.

hadoop com.sun.tools.javac.Main MutualFriends.java
jar cf wc.jar MutualFriends*.class
hadoop jar wc.jar MutualFriends /user/anirud/input /user/anirud/output

_________________________________________________________
Here /user/anirud/input specifies the path where the input file is stored. Make sure that the input file soc-LiveJournal1Adj.txt is present in this path.
/user/anirud/output is the path where the output of reducer is stored. You can specify any path for this.

_________________________________________________________
Also make sure that below paths are set in ~/.profile file
# set PATH so it includes user's private bin directories
PATH="$HOME/bin:$HOME/.local/bin:$PATH"
export JAVA_HOME=/usr/lib/java
export JRE_HOME=$JAVA_HOME/jre
export CLASSPATH=$CLASSPATH:.:$JAVA_HOME/lib:$JRE_HOME/lib
export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH
export PATH=$PATH:~/Documents/hadoop/bin:~/Documents/hadoop/sbin
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

__________________________________________________________
part-r-00000 contains the generated output.


