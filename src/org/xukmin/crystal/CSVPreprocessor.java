/**
 * Created By: Min Xu <xukmin@gmail.com>
 * Date: Jun 1, 2015
 */

package org.xukmin.crystal;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * This class Converts the CSV file with multirow records to single-row
 * records, by removing all quotes, as well as newlines and commas in quotes.
 *
 * This is to make the CSV file suitable as input to MapReduce.
 */
public class CSVPreprocessor {
  private static final char LF = '\n';
  private static final char CR = '\r';
  private static final char SPACE = ' ';
  private static final char SEPARATOR = ',';
  private static final char QUOTE = '"';

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println("Usage: CSVPreprocessor <input> <output>");
      return;
    }

    String input = args[0];
    String output = args[1];

    try (
      BufferedReader reader = new BufferedReader(new FileReader(input));
      PrintWriter writer = new PrintWriter(new FileWriter(output));
    ) {
      int value;
      StringBuffer sb = new StringBuffer();
      boolean inQuote = false;
      while ((value = reader.read()) != -1) {
        char c = (char) value;
        if (c == QUOTE) {
          if (sb.length() > 0 && sb.charAt(sb.length() - 1) == QUOTE) {
            sb.deleteCharAt(sb.length() - 1);
          }
          inQuote = !inQuote;
        } else if (inQuote) {
          switch (c) {
          case CR:
          case LF:
          case SEPARATOR:
            sb.append(SPACE);
            break;
          default:
            sb.append(c);
          }
        } else {
          switch (c) {
          case SEPARATOR:
            writer.print(sb.toString());
            writer.print(SEPARATOR);
            sb.setLength(0);
            break;
          case LF:
            writer.println(sb.toString());
            sb.setLength(0);
            break;
          case CR:
            break;
          default:
            sb.append(c);
          }
        }
      }
    }
  }
}
