package utils;

import java.util.TreeSet;

/**
 * @author Elena Druzhilova
 * @since 4/11/2018
 */
public class StringUtils {

    /**
     * Retrives only characters in words.
     *
     * @param word string contains word
     * @return clearified string
     */
    public static String cleanWords(String word) {
        return word.replaceAll("[^A-Za-z]+", "");
    }

    /**
     * @param token   it's size will be checked is it greater than tokens stored in
     * @param treeSet elements
     * @return true  if token size greater than TreeSet elements, false - otherwise.
     */
    public static boolean isTokenSizeGreaterTreeTokens(String token, TreeSet<String> treeSet) {
        if (treeSet.size() == 0 ||
                (token.length() > 0 && token.length() > treeSet.last().length())) {
            return true;
        }
        return false;
    }

    /**
     * @param token            will be added to treeSet if token's size is greater than treeSet tokens
     * @param treeSet          there are sorted elements that's will be compared with the token
     * @param longestWordCount treeSet size
     * @return                  resulted treeSet
     */
    public static TreeSet<String> addMaximumTokenToTreeSet(String token, TreeSet<String> treeSet, int longestWordCount) {
        if (token != null && !token.isEmpty()) {

            if (treeSet.size() < longestWordCount || isTokenSizeGreaterTreeTokens(token, treeSet)) {
                treeSet.add(token);

                if (treeSet.size() > longestWordCount) {
                    treeSet.remove(treeSet.first());
                }
            }
        }
        return treeSet;
    }

}
