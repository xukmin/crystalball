/**
 * Created By: Min Xu <xukmin@gmail.com>
 * Date: Jun 1, 2015
 */
package org.xukmin.crystal;

import java.util.ArrayList;
import java.util.List;

/**
 * A utility class to extract bigrams given a list of words.
 */
public class TextUtils {
  public static List<String> getBigrams(String[] words) {
    List<String> list = new ArrayList<String>();
    for (int i = 0; i < words.length - 1; i++) {
      list.add(String.format("%s %s",  words[i], words[i + i]));
    }
    return list;
  }
}
