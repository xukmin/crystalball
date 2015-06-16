CrystalBall - A Naive Bayes Text Classifier in Apache Hadoop MapReduce
======================================================================

* Created By: Min Xu <xukmin@gmail.com>
* Date: Jun 1, 2015

How to Use the Program
----------------------

### Build the Program

    ./build.sh

A jar file crystal.jar will be created in `bin/`.

### Build the Classification Model with MapReduce

    ./mapreduce.sh

Or manually specify the training data and output directory:

    hadoop jar bin/crystal.jar \
        org.xukmin.crystal.PostMapReduce \
        <training-data-on-HDFS> <output-directory-on-HDFS>

### Run the Classifier

    ./predict.sh

Or manually specify the MapReduce output directory and test data:

    java -cp bin/crystal.jar:$(hadoop classpath) \
        org.xukmin.crystal.PostPredictor \
        <mapreduce-output-directory-on-HDFS> <test-data-on-HDFS>

It reports precision, recall, F-measure and accuracy of the classifier.

### Run Benchmark

WARNING: The benchmark takes a long time and requires a lot of resources. The
script runs PostMapReduce for 150 times, with number of reducers set to 150,
149, 148, ..., 1.

    ./benchmark.sh

The benchmark data will be output to `benchmark.txt`. To draw a diagram:

    ./plot.gnuplot

The diagram will be output to `benchmark.png`.

### Clean the Binaries

    ./clean.sh

Program Structure
-----------------

### org.xukmin.crystal.CSVPreprocessor

Converts the CSV file with multirow records to single-row records, by removing
all quotes, as well as newlines and commas in quotes.

This is to make the CSV file suitable as input to MapReduce.

### org.xukmin.crystal.Post

Represents a post (question) on Stack Overflow. It parses a line from CSV and
creates an ArrayList internally to hold the post data.

### org.xukmin.crystal.PostMapper

Reads training records, tokenizes the body / title contents, removes
punctuations and stop words, and counts number of occurrences of each feature,
in open / closed / all posts respectively.

### org.xukmin.crystal.PostReducer

Aggregates and outputs the total values for each key. It also counts the number
of unique words in post body and title (i.e. the vocabulary size), and number
of unique tags through MapReduce counters.

### org.xukmin.crystal.PostMapReduce

A MapReduce which reads the training data set and builds the Naive Bayes model
by calculating all the parameters in the model.

### org.xukmin.crystal.Predictor

Applies the classifier. It reads output of `PostMapReduce` and loads all the
key-value pairs into a `HashMap<String, Long>`. For each post it parses,
PostPredictor goes through the selected features and calculates the probability
for each post status (“open” and “closed”) respectively, and selects the one
with the maximum probability as the predicted post status.  PostPredictor
eventually outputs all the statistics, number of posts that are predicted /
actually closed, precision, recall, F-measure, and accuracy.

