import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class problemThree {

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(problemThree.class);
        conf.setJobName("wordcount");

        conf.setOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);

        conf.setMapperClass(problemThreeMap.class);
        conf.setCombinerClass(ProblemThreeReduce.class);
        conf.setReducerClass(ProblemThreeReduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("S:\\assignment-3\\resources\\cleanTweets.json"),new Path("S:\\assignment-3\\resources\\reuter.json"));
        FileOutputFormat.setOutputPath(conf, new Path("S:\\assignment-3\\resources\\output\\"));

        JobClient.runJob(conf);
    }
}

