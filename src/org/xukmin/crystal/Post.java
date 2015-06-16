/**
 * Created By: Min Xu <xukmin@gmail.com>
 * Date: Jun 1, 2015
 */

package org.xukmin.crystal;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This class represents a post (question) on Stack Overflow.
 *
 * It parses a line from CSV and creates an ArrayList internally to hold the
 * post data.
 */
public class Post {
  public Post(String line) {
    post = new ArrayList<String>(Arrays.asList(line.split(",")));
    if (post.size() != NUM_COLUMNS) {
      throw new IllegalArgumentException();
    }
  }

  long getPostCreationDate() {
    try {
      return format.parse(post.get(POST_CREATION_DATE)).getTime();
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return 0;
  }

  long getOwnerCreationDate() {
    try {
      return format.parse(post.get(OWNER_CREATION_DATE)).getTime();
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return 0;
  }

  int getReputationAtPostCreation() {
    return Integer.valueOf(post.get(REPUTATION_AT_POST_CREATION));
  }

  int getOwnerUndeletedAnswerCountAtPostTime() {
    return Integer.valueOf(post.get(OWNER_UNDELETED_ANSWER_COUNT_AT_POST_TIME));
  }

  String getTitle() {
    return post.get(TITLE);
  }

  String getBody() {
    return post.get(BODY);
  }

  List<String> getTags() {
    List<String> tags = new ArrayList<String>(post.subList(TAG1, TAG5 + 1));
    tags.removeAll(Collections.singleton(""));
    return tags;
  }

  String getStatus() {
    // return post.get(STATUS);
    return post.get(STATUS).equals("open") ? "open" : "closed";
  }

  private List<String> post;

  private static final String DELIMITERS = "[\\p{Punct}\\s]+";

  private static final DateFormat format =
      new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

  @SuppressWarnings("unused")
  private static final int POST_ID = 0;
  private static final int POST_CREATION_DATE = 1;
  @SuppressWarnings("unused")
  private static final int OWNER_USER_ID = 2;
  private static final int OWNER_CREATION_DATE = 3;
  private static final int REPUTATION_AT_POST_CREATION = 4;
  private static final int OWNER_UNDELETED_ANSWER_COUNT_AT_POST_TIME = 5;
  private static final int TITLE = 6;
  private static final int BODY = 7;
  private static final int TAG1 = 8;
  private static final int TAG2 = 9;
  private static final int TAG3 = 10;
  private static final int TAG4 = 11;
  private static final int TAG5 = 12;
  @SuppressWarnings("unused")
  private static final int POST_CLOSE_DATE = 13;
  private static final int STATUS = 14;
  private static final int NUM_COLUMNS = 15;
}
