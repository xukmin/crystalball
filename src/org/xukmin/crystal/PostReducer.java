/**
 * Created By: Min Xu <xukmin@gmail.com>
 * Date: Jun 1, 2015
 */
package org.xukmin.crystal;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// TODO(xukmin): Add combiner to improve performance.

/**
 * This class aggregates and outputs the total values for each key.
 *
 * It also counts the number of unique words in post body and title (i.e. the
 * vocabulary size), and number of unique tags through MapReduce counters.
 */
public class PostReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
  public static enum Counters {
    UNIQUE_WORDS,
    UNIQUE_TAGS,
  }

  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    long count = 0;
    for (LongWritable value : values) {
      count += value.get();
    }
    context.write(key, new LongWritable(count));
    if (key.toString().indexOf("/") == -1) {
      context.getCounter(Counters.UNIQUE_WORDS).increment(1);
    } else if (key.toString().startsWith("/TAG/") &&
               key.toString().lastIndexOf("/") == 4) {
      context.getCounter(Counters.UNIQUE_TAGS).increment(1);
    }
  }
}
