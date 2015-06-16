/**
 * Created By: Min Xu <xukmin@gmail.com>
 * Date: Jun 1, 2015
 */
package org.xukmin.crystal;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class applies the Naive Bayes Classifier.
 *
 * It reads output of `PostMapReduce` and loads all the key-value pairs into a
 * `HashMap<String, Long>`. For each post it parses, PostPredictor goes through
 * the selected features and calculates the probability for each post status
 * (“open” and “closed”) respectively, and selects the one with the maximum
 * probability as the predicted post status.  PostPredictor eventually outputs
 * all the statistics, number of posts that are predicted / actually closed,
 * precision, recall, F-measure, and accuracy.
 */
public class PostPredictor {
  public PostPredictor(String directory)
      throws NumberFormatException, IOException {
    loadStopWords();
    loadCounters(directory);
    loadParameters(directory);
  }

  private void loadParameters(String directory) throws IOException {
    System.out.println("Loading Classification Model...");
    for (FileStatus file :
         fileSystem.globStatus(new Path(directory + "/part-r-*"))) {
      System.out.print(".");
      try (
        BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(
                    fileSystem.open(file.getPath())));
      ) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] kv = line.split("\t");
          if (kv.length != 2) {
            System.err.printf("Invalid line in %s.", file.getPath());
            System.exit(1);
          }
          parameters.put(kv[0], Long.valueOf(kv[1]));
        }
      }
    }
    System.out.println("");
    System.out.println("Classification Model loaded successfully.");
  }

  private void loadCounters(String directory) throws IOException {
    try (
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  fileSystem.open(new Path(directory + "/counters"))));
    ) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] kv = line.split("\t");
        if (kv.length != 2) {
          System.err.printf("Invalid line in counters file.\n");
          System.exit(1);
        }
        counters.put(kv[0], Long.valueOf(kv[1]));
      }
    }
  }

  private void loadStopWords() throws IOException {
    try (
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  fileSystem.open(new Path("stop-word-list.txt"))));
    ) {
      String word;
      while ((word = reader.readLine()) != null) {
        stopWords.add(word);
      }
    }
  }

  private void addWord(String word, int weight, long numWords,
                       Long[] numStates, double[] probabilities) {
    if (word.isEmpty()) {
      return;
    }

    if (stopWords.contains(word.toLowerCase())) {
      return;
    }

    for (int i = 0; i < states.length; i++) {
      Long numWordStatus = parameters.get(word + "/" + states[i]);
      if (numWordStatus == null) {
        numWordStatus = ZERO;
      }
      Long numWordsStatus = parameters.get("/WORDS_STATUS/" + states[i]);
      if (numWordsStatus == null) {
        numWordsStatus = ZERO;
      }
      // Laplace smoothing
      probabilities[i] +=
          weight * Math.log((numWordStatus + 1.0) / (numWordsStatus + numWords));
    }
  }

  private void addTag(String tag, int weight, long numUniqueTags,
                      Long[] numStates, double[] probabilities) {
    if (tag.isEmpty()) {
      return;
    }
    for (int i = 0; i < states.length; i++) {
      Long numWordStatus = parameters.get("/TAG/" + tag + "/" + states[i]);
      if (numWordStatus == null) {
        numWordStatus = ZERO;
      }
      Long numWordsStatus = parameters.get("/TAGS_STATUS/" + states[i]);
      if (numWordsStatus == null) {
        numWordsStatus = ZERO;
      }
      // Laplace smoothing
      probabilities[i] += weight * Math.log((numWordStatus + 1.0) /
          (numWordsStatus.longValue() + numUniqueTags));
    }
  }

  private void addNumber(String name, int weight, int bins,
                         Long[] numStates, double[] probabilities) {
    for (int i = 0; i < states.length; i++) {
      Long numWordStatus = parameters.get(name + "/" + states[i]);
      if (numWordStatus == null) {
        numWordStatus = ZERO;
      }
      probabilities[i] += weight * Math.log((numWordStatus + 1.0) /
          (numStates[i].longValue() + bins));
    }
  }

  public String predict(Post post) {
    double[] probabilities = new double[states.length];

    long numUniqueWords = counters.get(PostReducer.Counters.UNIQUE_WORDS.name());
    long numUniqueTags = counters.get(PostReducer.Counters.UNIQUE_TAGS.name());
    long numPosts = parameters.get("/POSTS");

    Long[] numStates = new Long[states.length];
    for (int i = 0; i < states.length; i++) {

      numStates[i] = parameters.get("/" + states[i]);
      if (numStates[i] == null) {
        System.err.print("numStates[" + i + "] is null.");
        System.exit(1);
      }
    }

    List<String> tags = post.getTags();
    String[] title = post.getTitle().split(DELIMITERS);
    String[] body = post.getBody().split(DELIMITERS);
    // For bigrams, use TextUtils.getBigrams(title).
    for (String word : title) {
      // Weight for title words is 2.
      addWord(word, 2, numUniqueWords, numStates, probabilities);
    }

    // For bigrams, use TextUtils.getBigrams(body).
    for (String word : body) {
      addWord(word, 1, numUniqueWords, numStates, probabilities);
    }

    for (String tag : tags) {
      addTag(tag, TAG_WEIGHT, numUniqueTags, numStates, probabilities);
    }

    int reputation = post.getReputationAtPostCreation();
    reputation = Math.max(reputation, -10);
    reputation = Math.min(reputation, 1000);
    int bin = (reputation + 10);
    addNumber("/REPUTATION/" + bin, REPUTATION_WEIGHT, 10010, numStates,
              probabilities);

    int undeleted = post.getOwnerUndeletedAnswerCountAtPostTime();
    undeleted = Math.max(undeleted, 0);
    undeleted = Math.min(undeleted, 1000);
    addNumber("/UNDELETED/" + undeleted, UNDELETED_WEIGHT, 1000, numStates,
              probabilities);

    long time = (post.getPostCreationDate() - post.getOwnerCreationDate()) /
        86400000;
    time = Math.min(time, 0);
    time = Math.max(time, 300);
    time /= 10;
    addNumber("/TIME/" + time, TIME_WEIGHT, 30, numStates, probabilities);

    for (int i = 0; i < states.length; i++) {
      probabilities[i] += Math.log(numStates[i] * 1.0 / numPosts);
    }

    int m = 0;
    for (int i = 0; i < probabilities.length; i++) {
      if (probabilities[i] > probabilities[m]) {
        m = i;
      }
    }

    return states[m];
  }

  private static double fMeasure(double beta, double precision, double recall) {
    return (1.0 + beta * beta) * precision * recall /
        (beta * beta * precision + recall);
  }

  public void predictAll(String file) throws NumberFormatException, IOException {
    try (
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  fileSystem.open(new Path(file))));
    ) {
      String line = reader.readLine();
      long numAccurate = 0;
      long numPredictClosedActualClosed = 0;
      long numPredictClosed = 0;
      long numActualClosed = 0;
      long all = 0;
      while ((line = reader.readLine()) != null) {
        Post post = new Post(line);
        String predict = predict(post);
        boolean actualClosed = !post.getStatus().equals("open");
        boolean predictClosed = !predict.equals("open");
        if (predictClosed == actualClosed) {
          numAccurate++;
        }
        if (predictClosed) {
          numPredictClosed++;
        }
        if (actualClosed) {
          numActualClosed++;
        }
        if (predictClosed && actualClosed) {
          numPredictClosedActualClosed++;
        }
        all++;
      }
      double precision = numPredictClosedActualClosed * 1.0 / numPredictClosed;
      double recall = numPredictClosedActualClosed * 1.0 / numActualClosed;

      System.out.printf("Predict Closed Actual Closed = %d\n",
                        numPredictClosedActualClosed);
      System.out.printf("Predict Closed               = %d\n", numPredictClosed);
      System.out.printf("Actual  Closed               = %d\n", numActualClosed);
      System.out.printf("Accurate                     = %d\n", numAccurate);
      System.out.printf("Posts                        = %d\n", all);

      System.out.printf("Precision = %f%%\n", 100.0 * precision);
      System.out.printf("Recall    = %f%%\n", 100.0 * recall);
      System.out.printf("F-measure = %f%%\n",
                        100.0 * fMeasure(1, precision, recall));
      System.out.printf("Accuracy  = %f%%\n", 100.0 * numAccurate / all);
    }
  }

  public static void main(String[] args)
      throws NumberFormatException, IOException {
    PostPredictor predictor = new PostPredictor(args[0]);
    predictor.predictAll(args[1]);
  }

  private static final String DELIMITERS = "[\\p{Punct}\\s]+";

  private FileSystem fileSystem = FileSystem.get(new Configuration());
  private HashSet<String> stopWords = new HashSet<String>();
  private HashMap<String, Long> counters = new HashMap<String, Long>();
  private HashMap<String, Long> parameters = new HashMap<String, Long>();

  private String[] states = new String[]{"open", "closed"};

  /*
  private String[] states = new String[]{
    "not a real question",
    "not constructive",
    "off topic",
    "open",
    "too localized"
  };
  */

  private static Long ZERO = new Long(0);

  private static int TAG_WEIGHT = 8;
  private static int REPUTATION_WEIGHT = 30;
  private static int UNDELETED_WEIGHT = 16;
  private static int TIME_WEIGHT = 0;
}
