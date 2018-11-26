package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.TreeSet;

import static utils.StringUtils.addMaximumTokenToTreeSet;
import static utils.StringUtils.cleanWords;

/**
 * Mapper class divides input lines into separate tokens.
 *
 * @author Elena Druzhilova
 * @since 4/11/2018
 */
public class LongestWordMapper extends Mapper<Object, Text, IntWritable, Text> {

    private String cleanedToken;

    private int longestWordCount;
    private TreeSet<String> uniqueSortedByLengthWords;
    private IntWritable wordSizeContext;
    private Text wordContext;

    @Override
    protected void setup(Context context) {
        longestWordCount = Integer.valueOf(context.getConfiguration().get("LongestWordCount"));
        uniqueSortedByLengthWords = new TreeSet<>(Comparator.comparingInt(String::length));
        wordSizeContext = new IntWritable();
        wordContext = new Text();
    }

    /**
     * Removes duplicates, converts words to lowercase, remains ony characters in a value,
     * looking for the longest words in a line and output only them.
     *
     * @param key     start position in a file
     * @param value   input line
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        while (tokenizer.hasMoreElements()) {
            cleanedToken = cleanWords(tokenizer.nextToken()).toLowerCase();
            uniqueSortedByLengthWords = addMaximumTokenToTreeSet(cleanedToken, uniqueSortedByLengthWords, longestWordCount);
        }

        uniqueSortedByLengthWords.forEach(word -> {
            wordSizeContext.set(word.length());
            wordContext.set(word);
            try {
                context.write(wordSizeContext, wordContext);
            } catch (Exception e) {
                new RuntimeException("Mapper cannot write to the context " + e.getMessage());
            }
        });
    }

    @Override
    protected void cleanup(Context context) {
        uniqueSortedByLengthWords.clear();
    }
}
