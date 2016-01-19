package io.github.infolis.util;

import io.github.infolis.InfolisConfig;
import io.github.infolis.algorithm.Indexer;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

/**
 * 
 * @author kata
 *
 */
public class RegexUtils {
	// maximum time for LimitedTimeMatcher
	// TODO set maxTimeMillis in config - its optimal value depends on granted stack size
	public static final long maxTimeMillis = 750000;
	// basic regex for extraction of numeric information
	public static final String percentRegex = new String("\\d+[.,]?\\d*%");
    public static final String enumRegex = "(([,;/&\\\\])|(and)|(und))";
    public static final String yearRegex = "(\\d{4})";
    public static final String yearAbbrRegex = "('\\d\\d)";
    public static final String numberRegex = "(\\d+[.,]?\\d*)"; // this includes yearRegex
    public static final String rangeRegex = "(([-–])|(bis)|(to)|(till)|(until))";
	public static final Pattern patternNumeric = Pattern.compile("\\d+");
	public static final Pattern patternDecimal = Pattern.compile("\\d+\\.\\d+");
    // complex regex for extraction of numeric information
    public static final String numericInfoRegex = "(" + yearRegex + "|" + yearAbbrRegex + "|" + numberRegex + ")";
    public static final String enumRangeRegex = "(" + enumRegex + "|" + rangeRegex + ")";
    public static final String complexNumericInfoRegex = "(" + numericInfoRegex + "(\\s*" + enumRangeRegex + "\\s*" + numericInfoRegex + ")*)";
    // sorted list of regex for extraction of numeric information (sorted by priority)
	public static final Pattern[] patterns = getContextMinerYearPatterns();
	// list of symbols to be treated as enumerators. Useful for querying textual references
	// TODO this feature seems to have been lost during refactoring of the matcher classes. Restore!
	public static final String[] enumeratorList = {",", ";", "/", "\\\\"};
	// regex for extracting URLs
	// TODO this feature seems to have been lost during refactoring. Restore!
	public static final String urlPatString = "((\\w+?://)|(www.*?\\.)).+\\.[a-zA-Z][a-zA-Z][a-zA-Z]*";
	public static final Pattern urlPat = Pattern.compile(urlPatString);

	// regex for extracting named entities
	// restricts names to contain at most 5 words (and at least 3 characters)
	public static final String studyRegex_ngram = new String("(\\S*?\\s?\\S+?\\s?\\S+?\\s?\\S+?\\s?\\S*?)");
	// word = any char sequence not containing whitespace (punctuation is seen as part of the word here)
	public static final String studyRegex = new String("(\\S+?)");
	public static final String wordRegex = new String("\\S+?");
	// use atomic grouping where possible to prevent catastrophic backtracking
	public static final String wordRegex_atomic = new String("\\S++");
	// use greedy variant for last word - normal wordRegex would only extract first character of last word
	public static final String lastWordRegex = new String("\\S+");

	// regex for extracting contexts of named entities
    public static final String leftContextPat_ = "((" + RegexUtils.wordRegex + ")\\s+(" + RegexUtils.wordRegex + ")\\s+(" + RegexUtils.wordRegex + ")\\s+(" + RegexUtils.wordRegex + ")\\s+(" + RegexUtils.wordRegex + ")\\s*?" + ")";
    public static final String rightContextPat_ = "(\\s*?(" + RegexUtils.wordRegex + ")\\s+(" + RegexUtils.wordRegex + ")\\s+(" + RegexUtils.wordRegex + ")\\s+(" + RegexUtils.wordRegex + ")\\s+(" + RegexUtils.lastWordRegex + "))";

    /**
     * Replaces regular expressions in term with placeholders. 
     * Used in TrainingSet class (and useful for weka exports)
     * 
     * @param term
     * @return
     */
	public static String normalizeRegex(String term)
	{
		Pattern yearPat = Pattern.compile(yearRegex);
		Pattern percentPat = Pattern.compile(percentRegex);
		Pattern numberPat = Pattern.compile(numberRegex);

		String yearNorm = new String("<YEAR>");
		String percentNorm = new String("<PERCENT>");
		String numberNorm = new String("<NUMBER>");

		// do not change order of replacements
		Matcher percentMatcher = percentPat.matcher(term);
		term = percentMatcher.replaceAll(percentNorm);

		Matcher yearMatcher = yearPat.matcher(term);
		term = yearMatcher.replaceAll(yearNorm);

		Matcher numberMatcher = numberPat.matcher(term);
		term = numberMatcher.replaceAll(numberNorm);

		return term;
	}

	/**
	 * Replaces placeholders for years, numbers and percent specifications (if previously inserted) with their
	 * regular expressions and quotes all parts of the regular expression that are to be treated as
	 * strings (all but character classes). Used in StandardPatternInducer class.
	 *
	 * @param string	input text where placeholders shall be replaced and all literals quoted
	 * @return			quoted regular expression string
	 */
	public static String normalizeAndEscapeRegex(String string) {
		String yearNorm = new String("<YEAR>");
		String percentNorm = new String("<PERCENT>");
		String numberNorm = new String("<NUMBER>");
		string = normalizeRegex(string);
		string = Pattern.quote(string).replace(yearNorm, "\\E" + yearRegex + "\\Q").replace(percentNorm, "\\E" + percentRegex + "\\Q").replace(numberNorm, "\\E" + numberRegex + "\\Q");
		return string;
	}

	/**
	 * Normalizes and escapes strings for usage as Lucene queries.
	 * Replaces placeholders by wildcards, removes characters with special meanings in Lucene and
	 * normalizes the query using the Lucene Analyzer used for building the Lucene index.
	 * Used in StandardPatternInducer class.
	 *
	 * @param string	input string to be used as Lucene query
	 * @return			a Lucene query string
	 */
	public static String normalizeAndEscapeRegex_lucene(String string)
	{
		string = string.trim();
		string = normalizeQuery(string, false);
		string = string.replaceAll(yearRegex, "*").replaceAll(percentRegex, "*").replaceAll(numberRegex, "*");
		return string;
	}

	/**
	 * Normalizes a query by applying a Lucene analyzer. Make sure the analyzer used here is the
	 * same as the analyzer used for indexing the text files! Used to normalize automatically 
	 * generated queries (e.g. in bootstrapping) that may contain special characters.
	 *
	 * @param 	query	the Lucene query to be normalized
	 * @return	a normalized version of the query
	 */
	@SuppressWarnings("deprecation")
	public static String normalizeQuery(String query, boolean quoteIfSpace)
	{
		Analyzer analyzer = Indexer.createAnalyzer();
		String field = "contents";
		String result = new String();
		TokenStream stream = analyzer.tokenStream(field, new StringReader(query));
		try
		{
			while (stream.incrementToken()) {
				result += " " + (stream.getAttribute(TermAttribute.class).term());
			}
		} catch (IOException e) {
			// not thrown due to using a string reader...
		}
		analyzer.close();
		if (quoteIfSpace && result.trim().matches(".*\\s.*")) {
				return "\"" + result.trim() + "\"";
		}
		return result.trim();
	}
	
	/**
	 * Returns a list of patterns for extracting numerical information.
	 *
	 * Patterns should be sorted by priority / reliability (highest priority first), first match is accepted
	 * by calling method. This way, you can give year specifications a higher weight than other
	 * number specifications, for example. Currently, only one pattern is used.
	 *
	 * @return	a list of patterns
	 */
	public static Pattern[] getContextMinerYearPatterns()
	{
		Pattern[] patterns = new Pattern[1];
		patterns[0] = Pattern.compile(complexNumericInfoRegex);
		return patterns;
	}

    /**
     * Checks whether a given word is a stop word
     *
     * @param word	arbitrary string sequence to be checked
     * @return	true if word is found to be a stop word, false otherwise
     */
    public static boolean isStopword(String word) {
        // word consists of punctuation, whitespace and digits only
        if (word.matches("[\\p{Punct}\\s\\d]*")) {
            return true;
        }
        // trim word, lower case and remove all punctuation
        word = word.replaceAll("\\p{Punct}+", "").trim().toLowerCase();
		// due to text extraction errors, whitespace is frequently added to words resulting in many single characters
        // TODO: use this as small work-around but work on better methods for automatic text correction
        if (word.length() < 2) {
            return true;
        }
        List<String> stopwords = InfolisConfig.getStopwords();
        if (stopwords.contains(word)) {
            return true;
        }
        // treat concatenations of two stopwords as stopword
        for (String stopword : stopwords) {
        	// replace with whitespace and use trim to avoid replacing occurrences inside of word, e.g.
        	// "Daten" -> replace "at" with "" would yield "den" -> stopword
            if (stopwords.contains(word.replace(stopword, " ").trim())) {
                return true;
            }
            if (word.replace(stopword, "").isEmpty()) {
                return true;
            }
        }
        return false;
    }

	public static boolean ignoreStudy(String studyname) {
		for (String ignorePattern : InfolisConfig.getIgnoreStudy()) {
			if (studyname.matches(ignorePattern)) {
				return true;
			}
		}
		return false;
	}


}
