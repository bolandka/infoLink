/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.github.infolis.algorithm;

import static org.junit.Assert.assertNotNull;
import io.github.infolis.infolink.luceneIndexing.InfolisBaseTest;
import io.github.infolis.model.Execution;
import io.github.infolis.model.InfolisFile;
import io.github.infolis.model.InfolisPattern;
import io.github.infolis.model.StudyContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kata
 * @author domi
 * @author kba
 */
public class FrequencyBasedBootstrappingTest extends InfolisBaseTest {

	Logger log = LoggerFactory.getLogger(FrequencyBasedBootstrappingTest.class);
	private List<String> uris = new ArrayList<>();
	private final static String term = "FOOBAR";
	private final static List<String> terms = Arrays.asList(term);

	public FrequencyBasedBootstrappingTest() throws Exception {
		for (InfolisFile file : createTestFiles(7)) {
			uris.add(file.getUri());
		}
	}

	/**
	 * Tests basic functionality using no threshold for pattern induction (=
	 * accept all). For a more detailed test refer to patternLearner.LearnerTest
	 * class.
	 * 
	 * @param strategy
	 * @throws Exception
	 */
	void testFrequencyBasedBootstrapping(Execution.Strategy strategy) throws Exception {

		Execution execution = new Execution();
		execution.setAlgorithm(FrequencyBasedBootstrapping.class);
		execution.getTerms().addAll(terms);
		execution.setInputFiles(uris);
		execution.setSearchTerm(terms.get(0));
		execution.setThreshold(0.0);
		execution.setBootstrapStrategy(strategy);

		Algorithm algo = new FrequencyBasedBootstrapping();
		algo.setDataStoreClient(dataStoreClient);
		algo.setFileResolver(fileResolver);
		algo.setExecution(execution);
		algo.run();

		for (String s : execution.getStudyContexts()) {
			StudyContext studyContext = dataStoreClient.get(StudyContext.class, s);
			InfolisPattern pat = dataStoreClient.get(InfolisPattern.class, studyContext.getPattern());
			log.debug("Study Context:\n {}Pattern: {}", studyContext.toXML(), pat.getPatternRegex());
			assertNotNull("StudyContext must have pattern set!", studyContext.getPattern());
			assertNotNull("StudyContext must have term set!", studyContext.getTerm());
			assertNotNull("StudyContext must have file set!", studyContext.getFile());
		}
	}

	@Test
	public void testBootstrapping_basic() throws Exception {

		testFrequencyBasedBootstrapping(Execution.Strategy.separate);
		testFrequencyBasedBootstrapping(Execution.Strategy.mergeCurrent);
		testFrequencyBasedBootstrapping(Execution.Strategy.mergeNew);
		testFrequencyBasedBootstrapping(Execution.Strategy.mergeAll);
	}

}