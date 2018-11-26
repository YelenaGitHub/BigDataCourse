package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Comparator;
import java.util.TreeSet;

import static utils.StringUtils.addMaximumTokenToTreeSet;

/**
 * Reducer class selects the longest word among the keys.
 *
 * @author Elena Druzhilova
 * @since 4/11/2018
 */
public class LongestWordReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private IntWritable maxLength = new IntWritable(0);
    private Text longestWord = new Text();
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

    @Override
    protected void reduce(IntWritable wordLength, Iterable<Text> values, Context context) {
        values.forEach(word -> {
            uniqueSortedByLengthWords = addMaximumTokenToTreeSet(word.toString(), uniqueSortedByLengthWords, longestWordCount);
        });
    }

    @Override
    protected void cleanup(Context context) {
        uniqueSortedByLengthWords.forEach(word -> {
            wordSizeContext.set(word.length());
            wordContext.set(word);
            try {
                context.write(wordSizeContext, wordContext);
            } catch (Exception e) {
                new RuntimeException("Mapper cannot write to the context " + e.getMessage());
            }
        });

        uniqueSortedByLengthWords.clear();
    }

}