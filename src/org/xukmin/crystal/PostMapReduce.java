/**
 * Created by: Min Xu <xukmin@gmail.com>
 * Date: Jun 1, 2015
 */

package org.xukmin.crystal;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class reads the training data set and builds the Naive Bayes model by
 * calculating all the parameters in the model.
 */
public class PostMapReduce extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PostMapReduce(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws IOException, ClassNotFoundException,
      InterruptedException {
    String defaultInput = "train_October_9_2012_clean_99.csv";
    String defaultOutput = "output";

    String input = args.length >= 1 ? args[0] : defaultInput;
    String output = args.length >= 2 ? args[1] : defaultOutput;

    Path inputPath = new Path(input);
    Path outputPath = new Path(output);

    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true /* recursive */);
    }

    Job job = Job.getInstance(getConf(), "PostMapReduce");
    job.setJarByClass(PostMapReduce.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setMapperClass(PostMapper.class);
    job.setReducerClass(PostReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    long startTime = System.currentTimeMillis();
    boolean succeeded = job.waitForCompletion(true);
    long endTime = System.currentTimeMillis();

    if (succeeded) {
      System.err.println("SUCCESS!");
    } else {
      System.err.println("FAIL!");
    }

    System.out.printf("Execution Time (ms) = %d\n", endTime - startTime);

    Counters counters = job.getCounters();

    try (
      PrintWriter writer =
          new PrintWriter(
              new OutputStreamWriter(
                  fs.create(new Path(defaultOutput + "/counters"), true)));
    ) {
      writer.printf("%s\t%d\n",
          PostReducer.Counters.UNIQUE_WORDS.name(),
          counters.findCounter(PostReducer.Counters.UNIQUE_WORDS).getValue());
      writer.printf("%s\t%d\n",
          PostReducer.Counters.UNIQUE_TAGS.name(),
          counters.findCounter(PostReducer.Counters.UNIQUE_TAGS).getValue());
    }

    return succeeded ? 0 : 1;
  }
}
