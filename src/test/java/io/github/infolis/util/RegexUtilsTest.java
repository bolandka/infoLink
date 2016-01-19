package io.github.infolis.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.regex.Pattern;

import org.junit.Test;

/**
 * 
 * @author kata
 *
 */
public class RegexUtilsTest {

	@Test
	public void testComplexNumericInfoRegex() throws Exception {
		Pattern pat = Pattern.compile(RegexUtils.complexNumericInfoRegex);
		assertThat(pat, is(not(nullValue())));
		assertThat(pat.matcher("1995").matches(), is(true));
		assertThat(pat.matcher("1995-1998").matches(), is(true));
		assertThat(pat.matcher("1995 bis 1998").matches(), is(true));
		assertThat(pat.matcher("1995 to 1998").matches(), is(true));
		assertThat(pat.matcher("1995       till '98").matches(), is(true));

		assertThat(pat.matcher("NaN").matches(), is(false));
		assertThat(pat.matcher("(1998)").matches(), is(false));
	}

	@Test
	public void normalizeQueryTest() {
		assertEquals("term", RegexUtils.normalizeQuery("term", true));
		assertEquals("term", RegexUtils.normalizeQuery("term,", true));
		assertEquals("term", RegexUtils.normalizeQuery(".term.", true));
		assertEquals("terma", RegexUtils.normalizeQuery("terma", true));

		assertEquals("\"the term\"", RegexUtils.normalizeQuery("the term", true));
		assertEquals("\"the term\"", RegexUtils.normalizeQuery("the term,", true));
		assertEquals("\"the term\"", RegexUtils.normalizeQuery(".the term.", true));
		assertEquals("\"the term\"", RegexUtils.normalizeQuery("the. term.", true));
	}

	@Test
	public void testIsStopword() {
		assertTrue(RegexUtils.isStopword("the"));
		assertTrue(RegexUtils.isStopword("thethe"));
		assertTrue(RegexUtils.isStopword("tothe"));
		assertTrue(RegexUtils.isStopword("e"));
		assertTrue(RegexUtils.isStopword("."));
		assertTrue(RegexUtils.isStopword(".the"));
		assertTrue(RegexUtils.isStopword("142"));
		assertTrue(RegexUtils.isStopword("142."));
		assertFalse(RegexUtils.isStopword("term"));
		assertFalse(RegexUtils.isStopword("theterm"));
		assertFalse(RegexUtils.isStopword("B142"));
		assertFalse(RegexUtils.isStopword("Daten"));
		assertTrue(RegexUtils.isStopword("für"));
	}

	@Test
	public void testNormalizeAndEscapeRegex() {
		assertEquals("\\Q\\E" + RegexUtils.percentRegex + "\\Q\\E", RegexUtils.normalizeAndEscapeRegex("2%"));
		assertEquals("\\Q\\E" + RegexUtils.numberRegex + "\\Q\\E", RegexUtils.normalizeAndEscapeRegex("2"));
		assertEquals("\\Q\\E" + RegexUtils.yearRegex + "\\Q\\E", RegexUtils.normalizeAndEscapeRegex("2000"));
	}
	
	//TODO may change if different values for ignoreStudy are set in the config
	@Test
	public void ignoreStudyTest() {
		assertTrue(RegexUtils.ignoreStudy("eigene Erhebung"));
		assertTrue(RegexUtils.ignoreStudy("eigene Erhebungen"));
		assertTrue(RegexUtils.ignoreStudy("eigene Berechnung"));
		assertTrue(RegexUtils.ignoreStudy("eigene Berechnungen"));
		assertTrue(RegexUtils.ignoreStudy("eigene Darstellung"));
		assertTrue(RegexUtils.ignoreStudy("eigene Darstellungen"));
		assertFalse(RegexUtils.ignoreStudy("ALLBUS"));
		assertFalse(RegexUtils.ignoreStudy("eigene Berechnung; ALLBUS"));
		assertFalse(RegexUtils.ignoreStudy("ALLBUS; eigene Berechnung"));
	}


}
