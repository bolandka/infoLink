package io.github.infolis.infolink.querying;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import io.github.infolis.algorithm.SearchResultLinker;
import io.github.infolis.algorithm.MultiMatchesLinker;
import io.github.infolis.model.entity.Entity;
import io.github.infolis.infolink.querying.QueryService.QueryField;
import io.github.infolis.infolink.querying.ExpectedOutput;

/**
 * 
 * @author kata
 *
 */
public class DaraHTMLQueryServiceTest {
	
	public static Set<ExpectedOutput> getExpectedOutput() {
		QueryService queryService = new DaraHTMLQueryService();
		queryService.setMaxNumber(27);
		Entity entity = new Entity();
		entity.setName("Studierendensurvey");
		Class<? extends SearchResultLinker> searchResultLinkerClass = MultiMatchesLinker.class;
		Set<ExpectedOutput> expectedOutput = new HashSet<ExpectedOutput>();
		Map<String, String> expectedDoiTitleMap = new HashMap<String, String>();
		expectedDoiTitleMap.put("10.4232/1.4263", "Studiensituation und studentische Orientierungen 2006/07 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12507" ,"Studiensituation und studentische Orientierungen 2006/07 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.4344", "Studiensituation und studentische Orientierungen 2003/04 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12508", "Studiensituation und studentische Orientierungen 2003/04 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.11059", "Studiensituation und studentische Orientierungen 2009/10 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12509", "Studiensituation und studentische Orientierungen 2009/10 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.1884", "Studiensituation und studentische Orientierungen 1982/83 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12499", "Studiensituation und studentische Orientierungen 1982/83 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.1885", "Studiensituation und studentische Orientierungen 1984/85 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12500", "Studiensituation und studentische Orientierungen 1984/85 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.3130", "Studiensituation und studentische Orientierungen 1992/93 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12503" ,"Studiensituation und studentische Orientierungen 1992/93 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.3131", "Studiensituation und studentische Orientierungen 1994/95 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12504", "Studiensituation und studentische Orientierungen 1994/95 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.2416", "Studiensituation und studentische Orientierungen 1986/87 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12501", "Studiensituation und studentische Orientierungen 1986/87 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.2416", "Studiensituation und studentische Orientierungen 1986/87 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.2417", "Studiensituation und studentische Orientierungen 1989/90 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12502", "Studiensituation und studentische Orientierungen 1989/90 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.5126", "Studiensituation und studentische Orientierungen 2012/13 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12510", "Studiensituation und studentische Orientierungen 2012/13 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.4208", "Studiensituation und studentische Orientierungen 2000/01 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12506", "Studiensituation und studentische Orientierungen 2000/01 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.3511", "Studiensituation und studentische Orientierungen 1997/98 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12505", "Studiensituation und studentische Orientierungen 1997/98 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12494", "Studiensituation und studentische Orientierungen (Studierenden-Survey) Kumulation 1983 - 2013");
		expectedDoiTitleMap.put("10.4232/1.12967", "Studiensituation und studentische Orientierungen 2015/16 (Studierenden-Survey)");
		expectedDoiTitleMap.put("10.4232/1.12974", "Studiensituation und studentische Orientierungen (Studierenden-Survey) Kumulation 1983 - 2016");

		ExpectedOutput output = new ExpectedOutput(queryService, entity, searchResultLinkerClass, expectedDoiTitleMap);
		expectedOutput.add(output);

		QueryService queryService2 = new DaraHTMLQueryService();
		queryService2.setMaxNumber(70);
		Entity entity2 = new Entity();
		entity2.setName("SOEP 2016");
		entity2.setNumericInfo(Arrays.asList("2016"));
		entity2.setYear("2016");
	
		Map<String, String> expectedDoiTitleMap2 = new HashMap<String, String>();
		expectedDoiTitleMap2.put("10.5684/k2id-soep-2013-15/v1", "K²ID-SOEP extension study");	
		expectedDoiTitleMap2.put("10.5684/soep.v33","Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2016");
		expectedDoiTitleMap2.put("10.5684/soep.v33i","Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2016 (internationale Version)");
		expectedDoiTitleMap2.put("10.7478/s0549.1.v1","SOEP-LEE Betriebsbefragung");
		expectedDoiTitleMap2.put("10.5684/soep.v33.1","Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2016");
		expectedDoiTitleMap2.put("10.5684/soep.v33.1i","Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2016 (internationale Version)");
		ExpectedOutput output2 = new ExpectedOutput(queryService2, entity2, searchResultLinkerClass, expectedDoiTitleMap2);
		expectedOutput.add(output2);

		return expectedOutput;
	}
	
	 @Test
	 public void testCreateTitleQuery() throws IOException {
		 QueryService queryService = new DaraHTMLQueryService();
	     queryService.setMaxNumber(600);
	     Set<QueryField> queryStrategy = new HashSet<>();
	     queryStrategy.add(QueryField.title);
	     queryService.setQueryStrategy(queryStrategy);
	     Entity entity = new Entity();
	     entity.setName("Studierendensurvey");
	     Assert.assertEquals(new URL("http://www.da-ra.de/dara/search/search_result?q=title:Studierendensurvey+resourceType:Dataset&lang=en&mdlang=de&max=600"), queryService.createQuery(entity));
	 }
	 
	 @Test
	 public void testCreateNumInTitleQuery() throws IOException {
		 QueryService queryService = new DaraHTMLQueryService();
	     queryService.setMaxNumber(600);
	     Set<QueryField> queryStrategy = new HashSet<>();
	     queryStrategy.add(QueryField.numericInfoInTitle);
	     queryService.setQueryStrategy(queryStrategy);
	     Entity entity = new Entity();
	     entity.setName("Studierendensurvey");
	     Assert.assertEquals(new URL("http://www.da-ra.de/dara/search/search_result?q=title:Studierendensurvey+resourceType:Dataset&lang=en&mdlang=de&max=600"), queryService.createQuery(entity));
	 }
}
