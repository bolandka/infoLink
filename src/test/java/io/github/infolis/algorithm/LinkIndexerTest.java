package io.github.infolis.algorithm;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import javax.json.JsonObject;

import org.junit.Test;
import org.junit.Ignore;

import io.github.infolis.InfolisBaseTest;
import io.github.infolis.model.EntityType;
import io.github.infolis.model.Execution;
import io.github.infolis.model.TextualReference;
import io.github.infolis.model.entity.Entity;
import io.github.infolis.model.entity.EntityLink;
import io.github.infolis.model.entity.EntityLink.EntityRelation;

public class LinkIndexerTest extends InfolisBaseTest {
	
	private String[] field;
	@Test
	public void test() {
		Execution exec = new Execution(LinkIndexer.class);
		EntityLink link1 = new EntityLink();
		EntityLink link2 = new EntityLink();
		Entity entity1 = new Entity();
		Entity entity2 = new Entity();
		Entity entity3 = new Entity();
		entity1.setIdentifiers(Arrays.asList("pub1"));
		entity2.setIdentifiers(Arrays.asList("cit1"));
		entity3.setIdentifiers(Arrays.asList("dat1"));
		entity1.setEntityType(EntityType.publication);
		entity2.setEntityType(EntityType.citedData);
		entity3.setEntityType(EntityType.dataset);
		dataStoreClient.post(Entity.class, entity1);
		dataStoreClient.post(Entity.class, entity2);
		dataStoreClient.post(Entity.class, entity3);
		link1.setFromEntity(entity1.getUri());
		link1.setToEntity(entity2.getUri());
		link2.setFromEntity(entity2.getUri());
		link2.setToEntity(entity3.getUri());
		link1.setConfidence(0.5);
		link2.setConfidence(0.9);
		link1.setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.references)));
		link2.setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_spatial, EntityRelation.part_of_temporal)));
		TextualReference ref = new TextualReference();
		ref.setLeftText("left text");
		ref.setRightText("right text");
		ref.setReference("reference");
		ref.setReferenceReliability(0.5);
		dataStoreClient.post(TextualReference.class, ref);
		link1.setLinkReason(ref.getUri());
		dataStoreClient.post(EntityLink.class, link1);
		dataStoreClient.post(EntityLink.class, link2);
		List<String> links = Arrays.asList(link1.getUri(), link2.getUri());
		exec.setLinks(links);
		exec.instantiateAlgorithm(dataStoreClient, fileResolver).run();
		// TODO tests
		LinkIndexer indexer = new LinkIndexer(dataStoreClient, dataStoreClient, fileResolver, fileResolver);
		indexer.setExecution(exec);
		List<EntityLink> listetityLink = indexer.flattenLinks(Arrays.asList(link1,link2));
		org.junit.Assert.assertEquals(1,listetityLink.size());
		//org.junit.Assert.assertEquals(, listetityLink.get(0).getEntityType());
	}
	
	@Ignore
	public void testSameAs() {
		
		// Ausgang: http://svkolodtest.gesis.intra/link-db/api/entityLink?q=fromEntity:ee0f4650-2e2b-11e8-a68c-895b49d4c31a
		
		Execution exec = new Execution(LinkIndexer.class);
		
		// Entity's: Entity e1 = new Entity();
		Entity[] entityList = new Entity[7];
		
		entityList[0].setIdentifiers(Arrays.asList("ee0f4650-2e2b-11e8-a68c-895b49d4c31a"));
		entityList[0].setAuthors(Arrays.asList("Haun,Dietmar","Haun,Dietmar"));
		entityList[0].setEntityProvenance("ALLBUS Bibliographie");
		entityList[0].setEntityReliability(5.0E-1); //?
		entityList[0].setEntityType(EntityType.publication);
		entityList[0].setEntityView("Müller,Walter, Haun,Dietmar (1994). Bildungsungleichheit im sozialen Wandel");
		entityList[0].setLanguage("de");
		entityList[0].setName("Bildungsungleichheit im sozialen Wandel");
		entityList[0].setPublicationStatus("IN FILE");
		entityList[0].setPublicationType("JOUR");
		entityList[0].setVolume("46");
		entityList[0].setYear("1994");
		
		entityList[1].setIdentifiers(Arrays.asList("eb485e20-2e2b-11e8-b934-a5c326a21c9e"));
		entityList[1].setEntityProvenance("ALLBUS Bibliographie");
		entityList[1].setEntityReliability(1); //?
		entityList[1].setEntityType(EntityType.citedData);
		entityList[1].setEntityView("ALLBUS kum 1980-1992");
		entityList[1].setName("ALLBUS kum");
		entityList[1].setNumericInfo(Arrays.asList("1980","1980-1992"));

		entityList[2].setIdentifiers(Arrays.asList("ee0b0090-2e2b-11e8-a68c-895b49d4c31a"));
		entityList[2].setEntityProvenance("ALLBUS Bibliographie");
		entityList[2].setEntityReliability(1); //?
		entityList[2].setEntityType(EntityType.citedData);
		entityList[2].setEntityView("SOEP 1986");
		entityList[2].setName("SOEP");
		entityList[2].setNumericInfo(Arrays.asList("1986"));
		
		entityList[3].setIdentifiers(Arrays.asList("ed1fdd40-2e2b-11e8-b934-a5c326a21c9e"));
		entityList[3].setEntityProvenance("ALLBUS Bibliographie");
		entityList[3].setEntityReliability(1); //?
		entityList[3].setEntityType(EntityType.citedData);
		entityList[3].setEntityView("Mikrozensus 1971");
		entityList[3].setName("Mikrozensus");
		entityList[3].setNumericInfo(Arrays.asList("1971"));
		
		entityList[4].setIdentifiers(Arrays.asList("f425ceb0-2e2b-11e8-b934-a5c326a21c9e"));
		entityList[4].setEntityProvenance("ALLBUS Bibliographie");
		entityList[4].setEntityReliability(1); //?
		entityList[4].setEntityType(EntityType.citedData);
		entityList[4].setEntityView("ALLBUS kum. 1980-1992");
		entityList[4].setName("ALLBUS kum.");
		entityList[4].setNumericInfo(Arrays.asList("1980","1980-1992"));
		
		entityList[5].setIdentifiers(Arrays.asList("ee0b0090-2e2b-11e8-a68c-895b49d4c31a"));
		entityList[5].setEntityProvenance("ALLBUS Bibliographie");
		entityList[5].setEntityReliability(1); //?
		entityList[5].setEntityType(EntityType.citedData);
		entityList[5].setEntityView("SOEP 1986");
		entityList[5].setName("SOEP");
		entityList[5].setNumericInfo(Arrays.asList("1986"));
		
		entityList[6].setIdentifiers(Arrays.asList("ed1fdd40-2e2b-11e8-b934-a5c326a21c9e"));
		entityList[6].setEntityProvenance("ALLBUS Bibliographie");
		entityList[6].setEntityReliability(1); //?
		entityList[6].setEntityType(EntityType.citedData);
		entityList[6].setEntityView("Mikrozensus 1971");
		entityList[6].setName("Mikrozensus");
		entityList[6].setNumericInfo(Arrays.asList("1971"));
		
		for ( int eLc = 1; eLc <= 7; eLc++ )
		{	
			dataStoreClient.post(Entity.class, entityList[eLc]);
		}
		
		// TextualReferece's: TextualReference ref = new TextualReference();
		TextualReference[] refList = new TextualReference[3];
				
		refList[0].setUri("http://svkolodtest.gesis.intra/link-db/api/textualReference/ff807440-2e2b-11e8-a68c-895b49d4c31a");
		refList[0].setLeftText("Im Unterschied zu der in der Literatur weithin verbreiteten These konstanter Ungleichheiten zeigt dieser Beitrag, dass seit der Zwischenkriegszeit und den ersten Nachkriegsjahren die Unterschiede zwischen den verschiedenen Bevölkerungsgruppen in der Bildungsbeteiligung und in den erworbenen Bildungsabschlüssen deutlich kleiner geworden sind. Die Analyse sukzessiver Übergänge zwischen den verschiedenen Stufen des Bildungswesens belegt, dass die Ungleichheit insbesondere durch den Abbau der sozialen Beteiligungsdifferentiale beim Übergang zu den weiterführenden Schulen und beim Erwerb der Mittleren Reife geringer geworden ist. Als Folge haben aber auch die Ungleichheiten beim Erwerb des Abiturs und von Hochschulabschlüssen abgenommen. Die Ungleichheitsreduktion ist unterschiedlich stark nach unterschiedlichen Ungleichheitsdimensionen, und sie variiert in unterschiedlichen Phasen der Nachkriegsentwicklung. Aus der Konstellation der Befunde werden spezifische Hypothesen zur Erklärung des Ungleichheitsabbaus diskutiert. Datenbasis der Analysen sind die kumulierten ALLBUS-Befragungen 1980-1992, das Sozioökonomische Panel 1986 und der Mikrozensus 1971");
		refList[0].setRightText("");
		refList[0].setReference("reference");
		refList[0].setReferenceReliability(0);
		
		refList[1].setUri("http://svkolodtest.gesis.intra/link-db/api/textualReference/fd000af0-2e2b-11e8-b934-a5c326a21c9e");
		refList[1].setLeftText("Im Unterschied zu der in der Literatur weithin verbreiteten These konstanter Ungleichheiten zeigt dieser Beitrag, dass seit der Zwischenkriegszeit und den ersten Nachkriegsjahren die Unterschiede zwischen den verschiedenen Bevölkerungsgruppen in der Bildungsbeteiligung und in den erworbenen Bildungsabschlüssen deutlich kleiner geworden sind. Die Analyse sukzessiver Übergänge zwischen den verschiedenen Stufen des Bildungswesens belegt, dass die Ungleichheit insbesondere durch den Abbau der sozialen Beteiligungsdifferentiale beim Übergang zu den weiterführenden Schulen und beim Erwerb der Mittleren Reife geringer geworden ist. Als Folge haben aber auch die Ungleichheiten beim Erwerb des Abiturs und von Hochschulabschlüssen abgenommen. Die Ungleichheitsreduktion ist unterschiedlich stark nach unterschiedlichen Ungleichheitsdimensionen, und sie variiert in unterschiedlichen Phasen der Nachkriegsentwicklung. Aus der Konstellation der Befunde werden spezifische Hypothesen zur Erklärung des Ungleichheitsabbaus diskutiert. Datenbasis der Analysen sind die kumulierten ALLBUS-Befragungen 1980-1992, das Sozioökonomische Panel 1986 und der Mikrozensus 1971");
		refList[1].setRightText("");
		refList[1].setReference("reference");
		refList[1].setReferenceReliability(0);
				
		refList[2].setUri("http://svkolodtest.gesis.intra/link-db/api/textualReference/fd6646d0-2e2b-11e8-b934-a5c326a21c9e");
		refList[2].setLeftText("Im Unterschied zu der in der Literatur weithin verbreiteten These konstanter Ungleichheiten zeigt dieser Beitrag, dass seit der Zwischenkriegszeit und den ersten Nachkriegsjahren die Unterschiede zwischen den verschiedenen Bevölkerungsgruppen in der Bildungsbeteiligung und in den erworbenen Bildungsabschlüssen deutlich kleiner geworden sind. Die Analyse sukzessiver Übergänge zwischen den verschiedenen Stufen des Bildungswesens belegt, dass die Ungleichheit insbesondere durch den Abbau der sozialen Beteiligungsdifferentiale beim Übergang zu den weiterführenden Schulen und beim Erwerb der Mittleren Reife geringer geworden ist. Als Folge haben aber auch die Ungleichheiten beim Erwerb des Abiturs und von Hochschulabschlüssen abgenommen. Die Ungleichheitsreduktion ist unterschiedlich stark nach unterschiedlichen Ungleichheitsdimensionen, und sie variiert in unterschiedlichen Phasen der Nachkriegsentwicklung. Aus der Konstellation der Befunde werden spezifische Hypothesen zur Erklärung des Ungleichheitsabbaus diskutiert. Datenbasis der Analysen sind die kumulierten ALLBUS-Befragungen 1980-1992, das Sozioökonomische Panel 1986 und der ");
		refList[2].setRightText("");
		refList[2].setReference("Mikrozensus 1971");
		refList[2].setReferenceReliability(0);
		
		for ( int rLc = 1; rLc <= 7; rLc++ )
		{	
			dataStoreClient.post(TextualReference.class, refList[rLc]);
		}
				
		// EntityLink's: EntityLink l1 = new EntityLink();
		EntityLink[] entityLinkList = new EntityLink[7];
		
		entityLinkList[0].setConfidence(1);
		entityLinkList[0].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.references))); // ? wieso so setzen?
		entityLinkList[0].setFromEntity(entityList[0].getUri());
		//entityLinkList[0].setLinkReason("http://svkolodtest.gesis.intra/link-db/api/textualReference/ff807440-2e2b-11e8-a68c-895b49d4c31a");
		entityLinkList[0].setLinkReason(refList[0].getUri());
		entityLinkList[0].setProvenance("ALLBUS Bibliographie");
		entityLinkList[0].setToEntity(entityList[1].getUri());
		
		entityLinkList[1].setConfidence(1);
		entityLinkList[1].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.references))); 
		entityLinkList[1].setFromEntity(entityList[0].getUri());
		//entityLinkList[1].setLinkReason("http://svkolodtest.gesis.intra/link-db/api/textualReference/fd000af0-2e2b-11e8-b934-a5c326a21c9e");
		entityLinkList[1].setLinkReason(refList[1].getUri());
		entityLinkList[1].setProvenance("ALLBUS Bibliographie");
		entityLinkList[1].setToEntity(entityList[2].getUri());
		
		entityLinkList[2].setConfidence(1);
		entityLinkList[2].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.references))); 
		entityLinkList[2].setFromEntity(entityList[0].getUri());
		//entityLinkList[2].setLinkReason("http://svkolodtest.gesis.intra/link-db/api/textualReference/fd6646d0-2e2b-11e8-b934-a5c326a21c9e");
		entityLinkList[2].setLinkReason(refList[2].getUri());
		entityLinkList[2].setProvenance("ALLBUS Bibliographie");
		entityLinkList[2].setToEntity(entityList[3].getUri());
		
		entityLinkList[3].setConfidence(1);
		entityLinkList[3].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.references))); 
		entityLinkList[3].setFromEntity(entityList[0].getUri());
		entityLinkList[3].setProvenance("ALLBUS Bibliographie ");
		entityLinkList[3].setToEntity(entityList[4].getUri());
		
		entityLinkList[4].setConfidence(1);
		entityLinkList[4].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.references))); 
		entityLinkList[4].setFromEntity(entityList[0].getUri());
		entityLinkList[4].setProvenance("ALLBUS Bibliographie");
		entityLinkList[4].setToEntity(entityList[5].getUri());
		
		entityLinkList[5].setConfidence(1);
		entityLinkList[5].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.references))); 
		entityLinkList[5].setFromEntity(entityList[0].getUri());
		entityLinkList[5].setProvenance("ALLBUS Bibliographie");
		entityLinkList[5].setToEntity(entityList[6].getUri());
		
		for ( int eLLc = 1; eLLc <= 7; eLLc++ )
		{	
			dataStoreClient.post(EntityLink.class, entityLinkList[eLLc]);
		}
		 
		List<String> links = Arrays.asList(entityLinkList[0].getUri(), entityLinkList[1].getUri(), entityLinkList[2].getUri(), entityLinkList[3].getUri(), entityLinkList[4].getUri(), entityLinkList[5].getUri());
		exec.setLinks(links);
		exec.instantiateAlgorithm(dataStoreClient, fileResolver).run();
		
		// TODO tests
		LinkIndexer indexer = new LinkIndexer(dataStoreClient, dataStoreClient, fileResolver, fileResolver);
		indexer.setExecution(exec);
		//List<EntityLink> listetityLink = indexer.flattenLinks(Arrays.asList(link1,link2));
		//org.junit.Assert.assertEquals(1,listetityLink.size());
		//org.junit.Assert.assertEquals(, listetityLink.get(0).getEntityType());
		
	}
	
	@Test
	public void TestIndexedQualityTotalPerType() throws Exception {
		// Set queryFields: 
		String[] queryFields = {"publication","dataset","instrument","citedData"};
		String RawQueryString = new String();
		String total = new String();
		
		// connect to ES Index (GWS)
		Execution exec = new Execution(Indexer.class);
		LinkIndexer indexer = new LinkIndexer(dataStoreClient, dataStoreClient, fileResolver, fileResolver);
		indexer.setExecution(exec);
		String dataSearchIndex = "http://svkolod01.gesis.intra:9200/link-db/_search";

		
		// Build queryStrings
		ArrayList<String> queryStrings = new ArrayList<String>();
		for ( String field: queryFields )
		{			
			//queryStrings.add("{\r  \"query\": {\r    \"bool\": {\r      \"must\": [\r         {\r          \"query_string\": {\r                \"default_field\" : \"entityType\",\r            \"query\": \""+field+"\"\r          }\r         }\r      ],\r      \"must_not\": [],\r      \"should\": []\r    }\r  }\r}\r");
			RawQueryString = ("{\r  \"query\": {\r    \"bool\": {\r      \"must\": [\r         {\r          \"query_string\": {\r                \"default_field\" : \"entityType\",\r            \"query\": \""+field+"\"\r          }\r         }\r      ],\r      \"must_not\": [],\r      \"should\": []\r    }\r  }\r}");
			JsonObject jsonObj = indexer.getJsonFromESIndex(dataSearchIndex, RawQueryString);	
			total = jsonObj.getJsonObject("hits").get("total").toString();
			
			// Check if more than 0 Entrys
			//total = "";
			org.junit.Assert.assertNotNull(total);
			org.junit.Assert.assertNotSame("0", total);
			org.junit.Assert.assertNotSame("", total);

			// Output
			System.out.println("--------------------------------------------------------------");
			//System.out.println(RawQueryString);
			System.out.println("Total Indexed Entities per Entry (" +field+ "): " + total);
		}
}

	@Test
	public void TestIndexedQualityTotalEntityLinksBetwenEntityTypes() throws Exception {
		// Set queryFields: 
		String[][] queryFields = {{"publication","dataset"},{"publication","citedData"},{"publication","instrument"},{"dataset","publication"},{"dataset","instrument"},{"instrument","dataset"},{"instrument","publication"}};
		//ArrayList<String> queryStrings = new ArrayList<String>();
		String RawQueryString = new String();
		String total = new String();
				
		// connect to ES Index (GWS)
		Execution exec = new Execution(Indexer.class);
		LinkIndexer indexer = new LinkIndexer(dataStoreClient, dataStoreClient, fileResolver, fileResolver);
		indexer.setExecution(exec);
		String dataSearchIndex = "http://svkolod01.gesis.intra:9200/link-db/_search";
				
		
		// Build queryStrings
		for ( String[] field: queryFields )
		{
			//System.out.println(field[0]+" - "+ field[1]);
		    RawQueryString = ("{\r  \"query\": {\r    \"bool\": {\r      \"must\": [\r         {\r          \"query_string\": {\r                \"default_field\" : \"gws_fromType\",\r            \"query\": \""+field[0]+"\"\r          }\r         },\r         {\r          \"query_string\": {\r                \"default_field\" : \"gws_toType\",\r            \"query\": \""+field[1]+"\"\r          }\r         }\r      ],\r      \"must_not\": [],\r      \"should\": []\r    }\r  }\r}");

			JsonObject jsonObj = indexer.getJsonFromESIndex(dataSearchIndex, RawQueryString);	
			total = jsonObj.getJsonObject("hits").get("total").toString();
			
			// Check if more than 0 Entrys
			//total = "";
			org.junit.Assert.assertNotNull(total);
			org.junit.Assert.assertNotSame("0", total);
			org.junit.Assert.assertNotSame("", total);
			
			// Output
			System.out.println("--------------------------------------------------------------");
			//System.out.println(RawQueryString);
			System.out.println("Total Indexed EntityLinks betwen EntityTypes ("+field[0]+" and "+field[1]+"): " + total);
		}
	}
}
