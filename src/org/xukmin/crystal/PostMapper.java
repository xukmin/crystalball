/**
 * Created By: Min Xu <xukmin@gmail.com>
 * Date: Jun 12, 2015
 */

package org.xukmin.crystal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class reads training records, tokenizes the body / title contents,
 * removes punctuations and stop words, and counts number of occurrences of
 * each feature, in open / closed / all posts respectively.
 */
public class PostMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  public static enum Counters {
    HEADERS,
    LINES,
    STOP_WORDS,
    STOP_WORDS_IN_BODY,
    STOP_WORDS_IN_TITLE,
  }

  private static final Text NUM_POSTS_KEY = new Text("/POSTS");
  private static final Text NUM_WORDS_KEY = new Text("/WORDS");

  private static final LongWritable ZERO = new LongWritable(0);
  private static final LongWritable ONE = new LongWritable(1);

  private static final LongWritable TITLE_WEIGHT = new LongWritable(1);
  private static final LongWritable TAG_WEIGHT = new LongWritable(1);

  private static final String DELIMITERS = "[\\p{Punct}\\s]+";

  private HashSet<String> stopWords = new HashSet<String>();

  @Override
  protected void setup(
      Mapper<LongWritable, Text, Text, LongWritable>.Context context)
      throws IOException, InterruptedException {
    super.setup(context);
    Path path = new Path("stop-word-list.txt");
    FileSystem fs = FileSystem.get(context.getConfiguration());
    try (
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(fs.open(path)));
    ) {
      String word;
      while ((word = reader.readLine()) != null) {
        stopWords.add(word);
      }
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws InterruptedException, IOException {
    if (key.equals(ZERO)) {
      context.getCounter(Counters.HEADERS).increment(1);
      return;
    }
    context.getCounter(Counters.LINES).increment(1);

    Post post = new Post(value.toString());
    String status = post.getStatus();
    int numWords = 0;
    // For bigrams, use TextUtils.getBigrams(post.getBody().split(DELIMITERS))).
    for (String word : post.getBody().split(DELIMITERS)) {
      if (word.isEmpty()) {
        continue;
      }
      if (stopWords.contains(word.toLowerCase())) {
        context.getCounter(Counters.STOP_WORDS_IN_BODY).increment(1);
        context.getCounter(Counters.STOP_WORDS).increment(1);
        continue;
      }
      context.write(new Text(word), ONE);
      context.write(new Text(word + "/" + status), ONE);
      numWords++;
    }

    // For bigrams, use TextUtils.getBigrams(post.getTitle().split(DELIMITERS))).
    for (String word : post.getTitle().split(DELIMITERS)) {
      if (word.isEmpty()) {
        continue;
      }
      if (stopWords.contains(word.toLowerCase())) {
        context.getCounter(Counters.STOP_WORDS_IN_TITLE).increment(1);
        context.getCounter(Counters.STOP_WORDS).increment(1);
        continue;
      }
      context.write(new Text(word), TITLE_WEIGHT);
      context.write(new Text(word + "/" + status), TITLE_WEIGHT);
      numWords += TITLE_WEIGHT.get();
    }

    List<String> tags = post.getTags();
    for (String tag : tags) {
      if (!tag.isEmpty()) {
        context.write(new Text("/TAG/" + tag), TAG_WEIGHT);
        context.write(new Text("/TAG/" + tag + "/" + status), TAG_WEIGHT);
        numWords += TAG_WEIGHT.get();
      }
    }
    context.write(new Text("/TAGS_PER_POST/" + tags.size() + "/" + status), ONE);
    context.write(new Text("/TAGS_STATUS/" + status), new LongWritable(tags.size()));

    int reputation = post.getReputationAtPostCreation();
    reputation = Math.max(reputation, -10);
    reputation = Math.min(reputation, 1000);
    int bin = (reputation + 10) / 1;
    context.write(new Text("/REPUTATION/" + bin + "/" + status), ONE);

    int undeleted = post.getOwnerUndeletedAnswerCountAtPostTime();
    undeleted = Math.max(undeleted, 0);
    undeleted = Math.min(undeleted, 1000);
    int undeletedBins = (undeleted) / 1;
    context.write(new Text("/UNDELETED/" + undeletedBins + "/" + status), ONE);

    long time = (post.getPostCreationDate() - post.getOwnerCreationDate()) / 86400000;
    time = Math.max(time, 0);
    time = Math.min(time, 300);
    time /= 10;
    context.write(new Text("/TIME/" + time + "/" + status), ONE);

    context.write(NUM_POSTS_KEY, ONE);
    context.write(NUM_WORDS_KEY,  new LongWritable(numWords));
    context.write(new Text("/" + status), ONE);
    context.write(new Text("/WORDS_STATUS/" + status), new LongWritable(numWords));
  }
}
