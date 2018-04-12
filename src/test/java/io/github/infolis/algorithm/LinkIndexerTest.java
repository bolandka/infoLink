package io.github.infolis.algorithm;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import javax.json.JsonObject;

import org.junit.Test;

import io.github.infolis.InfolisBaseTest;
import io.github.infolis.model.EntityType;
import io.github.infolis.model.Execution;
import io.github.infolis.model.TextualReference;
import io.github.infolis.model.entity.Entity;
import io.github.infolis.model.entity.EntityLink;
import io.github.infolis.model.entity.EntityLink.EntityRelation;

public class LinkIndexerTest extends InfolisBaseTest {
	
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
	
	@Test
	public void testSameAs() {
		
		// Ausgang: http://svkolodtest.gesis.intra/link-db/api/entityLink?q=fromEntity:ee0f4650-2e2b-11e8-a68c-895b49d4c31a
		
		Execution exec = new Execution(LinkIndexer.class);
		
		Entity[] entityList = new Entity[37];
		
		for ( int i = 0; i < 37; i++ ) entityList[i] = new Entity();

		// Publication:
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
		
		// CitedData
		entityList[1].setEntityProvenance("ALLBUS Bibliographie");
		entityList[1].setEntityReliability(1); //?
		entityList[1].setEntityType(EntityType.citedData);
		entityList[1].setEntityView("ALLBUS kum 1980-1992");
		entityList[1].setName("ALLBUS kum");
		entityList[1].setNumericInfo(Arrays.asList("1980","1980-1992"));

		entityList[2].setEntityProvenance("ALLBUS Bibliographie");
		entityList[2].setEntityReliability(1); //?
		entityList[2].setEntityType(EntityType.citedData);
		entityList[2].setEntityView("SOEP 1986");
		entityList[2].setName("SOEP");
		entityList[2].setNumericInfo(Arrays.asList("1986"));
		
		entityList[3].setEntityProvenance("ALLBUS Bibliographie");
		entityList[3].setEntityReliability(1); //?
		entityList[3].setEntityType(EntityType.citedData);
		entityList[3].setEntityView("Mikrozensus 1971");
		entityList[3].setName("Mikrozensus");
		entityList[3].setNumericInfo(Arrays.asList("1971"));
		
		entityList[4].setEntityProvenance("ALLBUS Bibliographie");
		entityList[4].setEntityReliability(1); //?
		entityList[4].setEntityType(EntityType.citedData);
		entityList[4].setEntityView("ALLBUS kum. 1980-1992");
		entityList[4].setName("ALLBUS kum.");
		entityList[4].setNumericInfo(Arrays.asList("1980","1980-1992"));
		
		entityList[5].setEntityProvenance("ALLBUS Bibliographie");
		entityList[5].setEntityReliability(1); //?
		entityList[5].setEntityType(EntityType.citedData);
		entityList[5].setEntityView("SOEP 1986");
		entityList[5].setName("SOEP");
		entityList[5].setNumericInfo(Arrays.asList("1986"));
		
		entityList[6].setEntityProvenance("ALLBUS Bibliographie");
		entityList[6].setEntityReliability(1); //?
		entityList[6].setEntityType(EntityType.citedData);
		entityList[6].setEntityView("Mikrozensus 1971");
		entityList[6].setName("Mikrozensus");
		entityList[6].setNumericInfo(Arrays.asList("1971"));
		
		// Dataset:
		
		//dc5421a0-2e2c-11e8-a68c-895b49d4c31a
		entityList[7].setAuthors(Arrays.asList("Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Schober, Pia S.", "Spieß, C. Katharina"));
		entityList[7].setDoi("10.5684/k2id-soep-2013-15/v1");
		entityList[7].setEntityProvenance("dataSearch");
		entityList[7].setEntityReliability(1); //?
		entityList[7].setEntityType(EntityType.dataset);
		entityList[7].setEntityView("Schober, Pia S.; Spieß, C. Katharina et al. (o.J.): K²ID-SOEP extension study");
		entityList[7].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de586796");
		entityList[7].setIdentifiers(Arrays.asList("10.5684/k2id-soep-2013-15/v1","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de586796","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de586796","httpwww.da-ra.deoaip--oaioai.da-ra.de586796"));
		entityList[7].setName("K²ID-SOEP extension study");
		entityList[7].setYear("o.J.");
		
		//dcac7ad0-2e2c-11e8-a68c-895b49d4c31a
		entityList[8].setAuthors(Arrays.asList("Liebig, Stefan","Schupp, Jürgen"));
		entityList[8].setDoi("10.7478/s0549.1.v1");
		entityList[8].setEntityProvenance("dataSearch");
		entityList[8].setEntityReliability(1); //?
		entityList[8].setEntityType(EntityType.dataset);
		entityList[8].setEntityView("Schupp, Jürgen; Liebig, Stefan (o.J.): SOEP-LEE Betriebsbefragung");
		entityList[8].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de294980");
		entityList[8].setIdentifiers(Arrays.asList("10.7478/s0549.1.v1","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de294980"," 	httpwww.da-ra.deoaip--oaioai.da-ra.de294980"));
		entityList[8].setName("SOEP-LEE Betriebsbefragung");
		entityList[8].setYear("o.J.");
		
		//dcb861b0-2e2c-11e8-a68c-895b49d4c31a
		entityList[9].setAuthors(Arrays.asList("Bartels, Charlotte","Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Erhardt, Klaudia","Fedorets, Alexandra","Franken, Andreas","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kröger, Hannes","Kuehne, Simon","Metzing, Maria","Nebelin, Jana","Richter, David","Schacht, Diana","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Wenzig, Knut"));
		entityList[9].setDoi("10.5684/soep.v33.1");
		entityList[9].setEntityProvenance("dataSearch");
		entityList[9].setEntityReliability(1); //?
		entityList[9].setEntityType(EntityType.dataset);
		entityList[9].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2016");
		entityList[9].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de615876");
		entityList[9].setIdentifiers(Arrays.asList("10.5684/soep.v33.1","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de615876","httpwww.da-ra.deoaip--oaioai.da-ra.de615876"));
		entityList[9].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2016");
		entityList[9].setYear("o.J.");
		
		//dcc0ed30-2e2c-11e8-a68c-895b49d4c31a
		entityList[10].setAuthors(Arrays.asList("Bartels, Charlotte","Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Erhardt, Klaudia","Fedorets, Alexandra","Franken, Andreas","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kröger, Hannes","Kuehne, Simon","Metzing, Maria","Nebelin, Jana","Richter, David","Schacht, Diana","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Wenzig, Knut"));
		entityList[10].setDoi("10.5684/soep.v33.1i");
		entityList[10].setEntityProvenance("dataSearch");
		entityList[10].setEntityReliability(1); //?
		entityList[10].setEntityType(EntityType.dataset);
		entityList[10].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2016 (internationale Version)");
		entityList[10].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de617810");
		entityList[10].setIdentifiers(Arrays.asList("10.5684/soep.v33.1i","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de617810","httpwww.da-ra.deoaip--oaioai.da-ra.de617810"));
		entityList[10].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2016 (internationale Version)");
		entityList[10].setYear("o.J.");

		//dcc978b0-2e2c-11e8-a68c-895b49d4c31a
		entityList[11].setAuthors(Arrays.asList("Bartsch, Simone","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Liebau, Elisabeth","Peter, Frauke","Richter, David","Schmitt, Christian","Schnitzlein, Daniel","Schupp, Jürgen","Tucci, Ingrid"));
		entityList[11].setDoi("10.5684/soep.v29");
		entityList[11].setEntityProvenance("dataSearch");
		entityList[11].setEntityReliability(1); //?
		entityList[11].setEntityType(EntityType.dataset);
		entityList[11].setEntityView("Schupp, Jürgen; Kroh, Martin et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2012");
		entityList[11].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de10072");
		entityList[11].setIdentifiers(Arrays.asList("10.5684/soep.v29","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de10072","httpwww.da-ra.deoaip--oaioai.da-ra.de10072"));
		entityList[11].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2012");
		entityList[11].setYear("o.J.");

		// Kopie von: 9!
		// dcb861b0-2e2c-11e8-a68c-895b49d4c31a
		
		//dcdb5300-2e2c-11e8-a68c-895b49d4c31a
		entityList[12].setAuthors(Arrays.asList("Bartels, Charlotte","Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Erhardt, Klaudia","Fedorets, Alexandra","Franken, Andreas","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kuehne, Simon","Richter, David","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Wenzig, Knut"));
		entityList[12].setDoi("10.5684/soep.v32.1");
		entityList[12].setEntityProvenance("dataSearch");
		entityList[12].setEntityReliability(1); //?
		entityList[12].setEntityType(EntityType.dataset);
		entityList[12].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2015");
		entityList[12].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de557535");
		entityList[12].setIdentifiers(Arrays.asList("10.5684/soep.v32.1","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de557535","httpwww.da-ra.deoaip--oaioai.da-ra.de557535"));
		entityList[12].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2015");
		entityList[12].setYear("o.J.");
		
		//dce453b0-2e2c-11e8-a68c-895b49d4c31a
		entityList[13].setAuthors(Arrays.asList("Bügelmayer, Elisabeth","Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kuehne, Simon","Liebau, Elisabeth","Richter, David","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Tucci, Ingrid","Wenzig, Knut"));
		entityList[13].setDoi("10.5684/soep.v30");
		entityList[13].setEntityProvenance("dataSearch");
		entityList[13].setEntityReliability(1); //?
		entityList[13].setEntityType(EntityType.dataset);
		entityList[13].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2013");
		entityList[13].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de431511");
		entityList[13].setIdentifiers(Arrays.asList("10.5684/soep.v30","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de431511","httpwww.da-ra.deoaip--oaioai.da-ra.de431511"));
		entityList[13].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2013");
		entityList[13].setYear("o.J.");
		
		//dcee17b0-2e2c-11e8-a68c-895b49d4c31a
		entityList[14].setAuthors(Arrays.asList("Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Erhardt, Klaudia","Fedorets, Alexandra","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kuehne, Simon","Priem, Maximilian","Richter, David","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Tucci, Ingrid","Wenzig, Knut"));
		entityList[14].setDoi("10.5684/soep.v31");
		entityList[14].setEntityProvenance("dataSearch");
		entityList[14].setEntityReliability(1); //?
		entityList[14].setEntityType(EntityType.dataset);
		entityList[14].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2014");
		entityList[14].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de464019");
		entityList[14].setIdentifiers(Arrays.asList("10.5684/soep.v31","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de464019","httpwww.da-ra.deoaip--oaioai.da-ra.de464019"));
		entityList[14].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2014");
		entityList[14].setYear("o.J.");
		
		//dcf829d0-2e2c-11e8-a68c-895b49d4c31a
		entityList[15].setAuthors(Arrays.asList("Bügelmayer, Elisabeth","Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kuehne, Simon","Liebau, Elisabeth","Richter, David","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Tucci, Ingrid","Wenzig, Knut"));
		entityList[15].setDoi("10.5684/soep.v30beta");
		entityList[15].setEntityProvenance("dataSearch");
		entityList[15].setEntityReliability(1); //?
		entityList[15].setEntityType(EntityType.dataset);
		entityList[15].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2013");
		entityList[15].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de427363");
		entityList[15].setIdentifiers(Arrays.asList("10.5684/soep.v30beta","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de427363","httpwww.da-ra.deoaip--oaioai.da-ra.de427363"));
		entityList[15].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2013");
		entityList[15].setYear("o.J.");
		
		//dd015190-2e2c-11e8-a68c-895b49d4c31a
		entityList[16].setAuthors(Arrays.asList("Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Erhardt, Klaudia","Fedorets, Alexandra","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kuehne, Simon","Priem, Maximilian","Richter, David","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Tucci, Ingrid","Wenzig, Knut"));
		entityList[16].setDoi("10.5684/soep.v31.1");
		entityList[16].setEntityProvenance("dataSearch");
		entityList[16].setEntityReliability(1); //?
		entityList[16].setEntityType(EntityType.dataset);
		entityList[16].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2014");
		entityList[16].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de470046");
		entityList[16].setIdentifiers(Arrays.asList("10.5684/soep.v31.1","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de470046","httpwww.da-ra.deoaip--oaioai.da-ra.de470046"));
		entityList[16].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2014");
		entityList[16].setYear("o.J.");
	
		//Kopie von: 12
		//dcdb5300-2e2c-11e8-a68c-895b49d4c31a
		
		//dd154ec0-2e2c-11e8-a68c-895b49d4c31a
		entityList[17].setAuthors(Arrays.asList("Anger, Silke","Frick, Joachim R.","Goebel, Jan","Grabka, Markus M.","Holst, Elke","Krause, Peter","Kroh, Martin","Lohmann, Henningn","Schmitt, Christian","Schupp, Jürgen","Spieß, C. Katharina","Wagner, Gert G."));
		entityList[17].setDoi("10.5684/soep.v25");
		entityList[17].setEntityProvenance("dataSearch");
		entityList[17].setEntityReliability(1); //?
		entityList[17].setEntityType(EntityType.dataset);
		entityList[17].setEntityView("Wagner, Gert G.; Frick, Joachim R. et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2008");
		entityList[17].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de2253");
		entityList[17].setIdentifiers(Arrays.asList("10.5684/soep.v25","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de2253","httpwww.da-ra.deoaip--oaioai.da-ra.de2253"));
		entityList[17].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2008");
		entityList[17].setYear("o.J.");
	
		//dd204b40-2e2c-11e8-a68c-895b49d4c31a
		entityList[18].setAuthors(Arrays.asList("Bartsch, Simone","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Liebau, Elisabeth","Peter, Frauke","Richter, David","Schmitt, Christian","Schnitzlein, Daniel","Schupp, Jürgen","Tucci, Ingrid"));
		entityList[18].setDoi("10.5684/soep.v29.1");
		entityList[18].setEntityProvenance("dataSearch");
		entityList[18].setEntityReliability(1); //?
		entityList[18].setEntityType(EntityType.dataset);
		entityList[18].setEntityView("Schupp, Jürgen; Kroh, Martin et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2012");
		entityList[18].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de291859");
		entityList[18].setIdentifiers(Arrays.asList("10.5684/soep.v29.1"," 	datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de291859","httpwww.da-ra.deoaip--oaioai.da-ra.de291859"));
		entityList[18].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2012");
		entityList[18].setYear("o.J.");
		
		//dd297300-2e2c-11e8-a68c-895b49d4c31a
		entityList[19].setAuthors(Arrays.asList("Bartels, Charlotte","Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Erhardt, Klaudia","Fedorets, Alexandra","Franken, Andreas","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kuehne, Simon","Richter, David","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Wenzig, Knut"));
		entityList[19].setDoi("10.5684/soep.v32i.1");
		entityList[19].setEntityProvenance("dataSearch");
		entityList[19].setEntityReliability(1); //?
		entityList[19].setEntityType(EntityType.dataset);
		entityList[19].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2015 (internationale Version)");
		entityList[19].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de557591");
		entityList[19].setIdentifiers(Arrays.asList("10.5684/soep.v32i.1","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de557591","httpwww.da-ra.deoaip--oaioai.da-ra.de557591"));
		entityList[19].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2015 (internationale Version)");
		entityList[19].setYear("o.J.");

		//dd32c1d0-2e2c-11e8-a68c-895b49d4c31a
		entityList[20].setAuthors(Arrays.asList("Bartels, Charlotte","Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Erhardt, Klaudia","Fedorets, Alexandra","Franken, Andreas","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kröger, Hannes","Kuehne, Simon","Metzing, Maria","Nebelin, Jana","Richter, David","Schacht, Diana","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Wenzig, Knut"));
		entityList[20].setDoi("10.5684/soep.v33i");
		entityList[20].setEntityProvenance("dataSearch");
		entityList[20].setEntityReliability(1); //?
		entityList[20].setEntityType(EntityType.dataset);
		entityList[20].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2016 (internationale Version)");
		entityList[20].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de605051");
		entityList[20].setIdentifiers(Arrays.asList(" 	10.5684/soep.v33i","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de605051","httpwww.da-ra.deoaip--oaioai.da-ra.de605051"));
		entityList[20].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2016 (internationale Version)");
		entityList[20].setYear("o.J.");
		
		//dd3bc280-2e2c-11e8-a68c-895b49d4c31a
		entityList[21].setAuthors(Arrays.asList("Anger, Silke","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Liebau, Elisabeth","Lohmann, Henning","Peter, Frauke","Richter, David","Schmitt, Christian","Schnitzlein, Daniel","Schupp, Jürgen","Tucci, Ingrid","Werneburg, Juliana"));
		entityList[21].setDoi("10.5684/soep.v28.1");
		entityList[21].setEntityProvenance("dataSearch");
		entityList[21].setEntityReliability(1); //?
		entityList[21].setEntityType(EntityType.dataset);
		entityList[21].setEntityView("Schupp, Jürgen; Kroh, Martin et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2011");
		entityList[21].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de8559");
		entityList[21].setIdentifiers(Arrays.asList("10.5684/soep.v28.1","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de8559","httpwww.da-ra.deoaip--oaioai.da-ra.de8559"));
		entityList[21].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2011");
		entityList[21].setYear("o.J.");

		//Kopie von: 20
		//dd3bc280-2e2c-11e8-a68c-895b49d4c31a
		
		//dd4b52e0-2e2c-11e8-a68c-895b49d4c31a
		entityList[22].setAuthors(Arrays.asList("Anger, Silke","Frick, Joachim R.","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Holst, Elke","Krause, Peter","Kroh, Martin","Liebau, Elisabeth","Lohmann, Henning","Richter, David","Schmitt, Christian","Schnitzlein, Daniel","Schupp, Jürgen","Spieß, C. Katharina","Wagner, Gert G."));
		entityList[22].setDoi("10.5684/soep.v27.1");
		entityList[22].setEntityProvenance("dataSearch");
		entityList[22].setEntityReliability(1); //?
		entityList[22].setEntityType(EntityType.dataset);
		entityList[22].setEntityView("Wagner, Gert G.; Frick, Joachim R. et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2010");
		entityList[22].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de2256");
		entityList[22].setIdentifiers(Arrays.asList("10.5684/soep.v27.1","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de2256","httpwww.da-ra.deoaip--oaioai.da-ra.de2256"));
		entityList[22].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2010");
		entityList[22].setYear("o.J.");

		//dd53b750-2e2c-11e8-a68c-895b49d4c31a
		entityList[23].setAuthors(Arrays.asList("Anger, Silke","Frick, Joachim R.","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Holst, Elke","Krause, Peter","Kroh, Martin","Liebau, Elisabeth","Lohmann, Henning","Richter, David","Schmitt, Christian","Schnitzlein, Daniel","Schupp, Jürgen","Wagner, Gert G.","Werneburg, Juliana"));
		entityList[23].setDoi("10.5684/soep.v27.2i");
		entityList[23].setEntityProvenance("dataSearch");
		entityList[23].setEntityReliability(1); //?
		entityList[23].setEntityType(EntityType.dataset);
		entityList[23].setEntityView("Wagner, Gert G.; Frick, Joachim R. et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2010");
		entityList[23].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de2258");
		entityList[23].setIdentifiers(Arrays.asList("10.5684/soep.v27.2i","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de2258","httpwww.da-ra.deoaip--oaioai.da-ra.de2258"));
		entityList[23].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2010");
		entityList[23].setYear("o.J.");		
		
		//Kopie von: 23
		//dd53b750-2e2c-11e8-a68c-895b49d4c31a
		
		//Kopie von: 22
		//dd4b52e0-2e2c-11e8-a68c-895b49d4c31a
		
		//dd6b0fe0-2e2c-11e8-a68c-895b49d4c31a
		entityList[24].setAuthors(Arrays.asList("Anger, Silke","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Liebau, Elisabeth","Lohmann, Henning","Peter, Frauke","Richter, David","Schmitt, Christian","Schnitzlein, Daniel","Schupp, Jürgen","Tucci, Ingrid","Werneburg, Juliana"));
		entityList[24].setDoi("10.5684/soep.v28.1i");
		entityList[24].setEntityProvenance("dataSearch");
		entityList[24].setEntityReliability(1); //?
		entityList[24].setEntityType(EntityType.dataset);
		entityList[24].setEntityView("Schupp, Jürgen; Kroh, Martin et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2011");
		entityList[24].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de8579");
		entityList[24].setIdentifiers(Arrays.asList("10.5684/soep.v28.1i","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de8579","httpwww.da-ra.deoaip--oaioai.da-ra.de8579"));
		entityList[24].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2011");
		entityList[24].setYear("o.J.");
		
		//dd7437a0-2e2c-11e8-a68c-895b49d4c31a
		entityList[25].setAuthors(Arrays.asList("Bügelmayer, Elisabeth","Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kuehne, Simon","Liebau, Elisabeth","Richter, David","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Tucci, Ingrid","Wenzig, Knut"));
		entityList[25].setDoi("10.5684/soep.v30ibeta");
		entityList[25].setEntityProvenance("dataSearch");
		entityList[25].setEntityReliability(1); //?
		entityList[25].setEntityType(EntityType.dataset);
		entityList[25].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2013 (internationale Version)");
		entityList[25].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de427362");
		entityList[25].setIdentifiers(Arrays.asList("10.5684/soep.v30ibeta","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de427362","httpwww.da-ra.deoaip--oaioai.da-ra.de427362"));
		entityList[25].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2013 (internationale Version)");
		entityList[25].setYear("o.J.");
		
		//dd7cc320-2e2c-11e8-a68c-895b49d4c31a
		entityList[26].setAuthors(Arrays.asList("Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Erhardt, Klaudia","Fedorets, Alexandra","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kuehne, Simon","Priem, Maximilian","Richter, David","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Tucci, Ingrid","Wenzig, Knut"));
		entityList[26].setDoi("10.5684/soep.v31.1i");
		entityList[26].setEntityProvenance("dataSearch");
		entityList[26].setEntityReliability(1); //?
		entityList[26].setEntityType(EntityType.dataset);
		entityList[26].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2014 (internationale Version)");
		entityList[26].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de470101");
		entityList[26].setIdentifiers(Arrays.asList("10.5684/soep.v31.1i","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de470101","httpwww.da-ra.deoaip--oaioai.da-ra.de470101"));
		entityList[26].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2014 (internationale Version)");
		entityList[26].setYear("o.J.");
		
		//Kopie von: 19
		//dd297300-2e2c-11e8-a68c-895b49d4c31a

		//dd8bde50-2e2c-11e8-a68c-895b49d4c31a
		entityList[27].setAuthors(Arrays.asList("Deutsches Institut für Wirtschaftsforschung (DIW Berlin)","Erhardt, Klaudia","Fedorets, Alexandra","Giesselmann, Marco","Goebel, Jan","Grabka, Markus","Krause, Peter","Kroh, Martin","Kuehne, Simon","Priem, Maximilian","Richter, David","Schmelzer, Paul","Schmitt, Christian","Schnitzlein, Daniel","Schröder, Carsten","Schupp, Jürgen","Siegers, Rainer","Tucci, Ingrid","Wenzig, Knut"));
		entityList[27].setDoi("10.5684/soep.v31i");
		entityList[27].setEntityProvenance("dataSearch");
		entityList[27].setEntityReliability(1); //?
		entityList[27].setEntityType(EntityType.dataset);
		entityList[27].setEntityView("Schupp, Jürgen; Goebel, Jan et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2014 (internationale Version)");
		entityList[27].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de464020");
		entityList[27].setIdentifiers(Arrays.asList("10.5684/soep.v31i","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de464020","httpwww.da-ra.deoaip--oaioai.da-ra.de464020"));
		entityList[27].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2014 (internationale Version)");
		entityList[27].setYear("o.J.");
		
		//dd95f070-2e2c-11e8-a68c-895b49d4c31a5
		entityList[28].setAuthors(Arrays.asList("Anger, Silke","Frick, Joachim R.","Goebel, Jan","Grabka, Markus M.","Groh-Samberg, Olaf","Holst, Elke","Krause, Peter","Kroh, Martin","Lohmann, Henningn","Pischner, Rainer","Schmitt, Christian","Schupp, Jürgen","Spieß, C. Katharina","Spieß, Martin","Wagner, Gert G."));
		entityList[28].setDoi("10.5684/soep.v24");
		entityList[28].setEntityProvenance("dataSearch");
		entityList[28].setEntityReliability(1); //?
		entityList[28].setEntityType(EntityType.dataset);
		entityList[28].setEntityView("Wagner, Gert G.; Frick, Joachim R. et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2007");
		entityList[28].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de2252");
		entityList[28].setIdentifiers(Arrays.asList("10.5684/soep.v24","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de2252","httpwww.da-ra.deoaip--oaioai.da-ra.de2252"));
		entityList[28].setName(" 	Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2007");
		entityList[28].setYear("o.J.");
		
		//dd9f8d60-2e2c-11e8-a68c-895b49d4c31a
		entityList[29].setAuthors(Arrays.asList("Anger, Silke","Frick, Joachim R.","Goebel, Jan","Grabka, Markus M.","Holst, Elke","Krause, Peter","Kroh, Martin","Liebau, Elisabeth","Lohmann, Henning","Schmitt, Christian","Schupp, Jürgen","Spieß, C. Katharina","Wagner, Gert G."));
		entityList[29].setDoi("10.5684/soep.v26");
		entityList[29].setEntityProvenance("dataSearch");
		entityList[29].setEntityReliability(1); //?
		entityList[29].setEntityType(EntityType.dataset);
		entityList[29].setEntityView("Wagner, Gert G.; Frick, Joachim R. et al. (o.J.): Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2009");
		entityList[29].setGwsId("datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de2254");
		entityList[29].setIdentifiers(Arrays.asList("10.5684/soep.v26","datasearch-httpwww-da-ra-deoaip--oaioai-da-ra-de2254","httpwww.da-ra.deoaip--oaioai.da-ra.de2254",""));
		entityList[29].setName("Sozio-oekonomisches Panel (SOEP), Daten der Jahre 1984-2009");
		entityList[29].setYear("o.J.");
		
		// Kopie von: 25
		//dd7437a0-2e2c-11e8-a68c-895b49d4c31a
	
		dataStoreClient.post(Entity.class, Arrays.asList(entityList));

		TextualReference[] refList = new TextualReference[3];
				
		for (int i = 0; i < 3; i++) refList[i] = new TextualReference();

		//refList[0].setUri("http://svkolodtest.gesis.intra/link-db/api/textualReference/ff807440-2e2b-11e8-a68c-895b49d4c31a");
		refList[0].setLeftText("Im Unterschied zu der in der Literatur weithin verbreiteten These konstanter Ungleichheiten zeigt dieser Beitrag, dass seit der Zwischenkriegszeit und den ersten Nachkriegsjahren die Unterschiede zwischen den verschiedenen Bevِlkerungsgruppen in der Bildungsbeteiligung und in den erworbenen Bildungsabschlüssen deutlich kleiner geworden sind. Die Analyse sukzessiver ـbergنnge zwischen den verschiedenen Stufen des Bildungswesens belegt, dass die Ungleichheit insbesondere durch den Abbau der sozialen Beteiligungsdifferentiale beim ـbergang zu den weiterführenden Schulen und beim Erwerb der Mittleren Reife geringer geworden ist. Als Folge haben aber auch die Ungleichheiten beim Erwerb des Abiturs und von Hochschulabschlüssen abgenommen. Die Ungleichheitsreduktion ist unterschiedlich stark nach unterschiedlichen Ungleichheitsdimensionen, und sie variiert in unterschiedlichen Phasen der Nachkriegsentwicklung. Aus der Konstellation der Befunde werden spezifische Hypothesen zur Erklنrung des Ungleichheitsabbaus diskutiert. Datenbasis der Analysen sind die kumulierten ALLBUS-Befragungen 1980-1992, das Sozioِkonomische Panel 1986 und der Mikrozensus 1971");
		refList[0].setRightText("");
		refList[0].setReference(""); 
		refList[0].setReferenceReliability(0);
		
		//refList[1].setUri("http://svkolodtest.gesis.intra/link-db/api/textualReference/fd000af0-2e2b-11e8-b934-a5c326a21c9e");
		refList[1].setLeftText("Im Unterschied zu der in der Literatur weithin verbreiteten These konstanter Ungleichheiten zeigt dieser Beitrag, dass seit der Zwischenkriegszeit und den ersten Nachkriegsjahren die Unterschiede zwischen den verschiedenen Bevِlkerungsgruppen in der Bildungsbeteiligung und in den erworbenen Bildungsabschlüssen deutlich kleiner geworden sind. Die Analyse sukzessiver ـbergنnge zwischen den verschiedenen Stufen des Bildungswesens belegt, dass die Ungleichheit insbesondere durch den Abbau der sozialen Beteiligungsdifferentiale beim ـbergang zu den weiterführenden Schulen und beim Erwerb der Mittleren Reife geringer geworden ist. Als Folge haben aber auch die Ungleichheiten beim Erwerb des Abiturs und von Hochschulabschlüssen abgenommen. Die Ungleichheitsreduktion ist unterschiedlich stark nach unterschiedlichen Ungleichheitsdimensionen, und sie variiert in unterschiedlichen Phasen der Nachkriegsentwicklung. Aus der Konstellation der Befunde werden spezifische Hypothesen zur Erklنrung des Ungleichheitsabbaus diskutiert. Datenbasis der Analysen sind die kumulierten ALLBUS-Befragungen 1980-1992, das Sozioِkonomische Panel 1986 und der Mikrozensus 1971");
		refList[1].setRightText("");
		refList[1].setReference("");
		refList[1].setReferenceReliability(0);
				
		//refList[2].setUri("http://svkolodtest.gesis.intra/link-db/api/textualReference/fd6646d0-2e2b-11e8-b934-a5c326a21c9e");
		refList[2].setLeftText("Im Unterschied zu der in der Literatur weithin verbreiteten These konstanter Ungleichheiten zeigt dieser Beitrag, dass seit der Zwischenkriegszeit und den ersten Nachkriegsjahren die Unterschiede zwischen den verschiedenen Bevِlkerungsgruppen in der Bildungsbeteiligung und in den erworbenen Bildungsabschlüssen deutlich kleiner geworden sind. Die Analyse sukzessiver ـbergنnge zwischen den verschiedenen Stufen des Bildungswesens belegt, dass die Ungleichheit insbesondere durch den Abbau der sozialen Beteiligungsdifferentiale beim ـbergang zu den weiterführenden Schulen und beim Erwerb der Mittleren Reife geringer geworden ist. Als Folge haben aber auch die Ungleichheiten beim Erwerb des Abiturs und von Hochschulabschlüssen abgenommen. Die Ungleichheitsreduktion ist unterschiedlich stark nach unterschiedlichen Ungleichheitsdimensionen, und sie variiert in unterschiedlichen Phasen der Nachkriegsentwicklung. Aus der Konstellation der Befunde werden spezifische Hypothesen zur Erklنrung des Ungleichheitsabbaus diskutiert. Datenbasis der Analysen sind die kumulierten ALLBUS-Befragungen 1980-1992, das Sozioِkonomische Panel 1986 und der ");
		refList[2].setRightText("");
		refList[2].setReference("Mikrozensus 1971");
		refList[2].setReferenceReliability(0);
		
		dataStoreClient.post(TextualReference.class, Arrays.asList(refList));
				
		EntityLink[] entityLinkList = new EntityLink[35];
		
		for (int i = 0; i < 35; i++) entityLinkList[i] = new EntityLink();
		
		// textualReference/ff807440-2e2b-11e8-a68c-895b49d4c31a
		entityLinkList[0].setConfidence(1);
		entityLinkList[0].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.references)));
		entityLinkList[0].setFromEntity(entityList[0].getUri());
		entityLinkList[0].setLinkReason(refList[0].getUri());
		entityLinkList[0].setProvenance("ALLBUS Bibliographie");
		entityLinkList[0].setToEntity(entityList[1].getUri());
		
		entityLinkList[1].setConfidence(1);
		entityLinkList[1].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.references))); 
		entityLinkList[1].setFromEntity(entityList[0].getUri());
		entityLinkList[1].setLinkReason(refList[1].getUri());
		entityLinkList[1].setProvenance("ALLBUS Bibliographie");
		entityLinkList[1].setToEntity(entityList[2].getUri());
		
		entityLinkList[2].setConfidence(1);
		entityLinkList[2].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.references))); 
		entityLinkList[2].setFromEntity(entityList[0].getUri());
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
		
		//EntityLinks: Dataset -> CitedData	http://svkolodtest.gesis.intra/link-db/api/entityLink?q=fromEntity:ee0b0090-2e2b-11e8-a68c-895b49d4c31a
		entityLinkList[6].setConfidence(6.5E-1);
		entityLinkList[6].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[6].setFromEntity(entityList[7].getUri());
		entityLinkList[6].setLinkReason("");
		entityLinkList[6].setProvenance("InfoLink");
		entityLinkList[6].setToEntity(entityList[8].getUri());
		
		entityLinkList[7].setConfidence(6.5E-1);
		entityLinkList[7].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[7].setFromEntity(entityList[7].getUri());
		entityLinkList[7].setLinkReason("");
		entityLinkList[7].setProvenance("InfoLink");
		entityLinkList[7].setToEntity(entityList[9].getUri());
		
		entityLinkList[8].setConfidence(9.0E-1);
		entityLinkList[8].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[8].setFromEntity(entityList[7].getUri());
		entityLinkList[8].setLinkReason("");
		entityLinkList[8].setProvenance("InfoLink");
		entityLinkList[8].setToEntity(entityList[10].getUri());
		
		entityLinkList[9].setConfidence(9.0E-1);
		entityLinkList[9].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[9].setFromEntity(entityList[7].getUri());
		entityLinkList[9].setLinkReason("");
		entityLinkList[9].setProvenance("InfoLink");
		entityLinkList[9].setToEntity(entityList[11].getUri());
		
		// *Kopie 9
		entityLinkList[10].setConfidence(9.0E-1);
		entityLinkList[10].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[10].setFromEntity(entityList[7].getUri());
		entityLinkList[10].setLinkReason("");
		entityLinkList[10].setProvenance("InfoLink");
		entityLinkList[10].setToEntity(entityList[9].getUri());
		
		entityLinkList[11].setConfidence(9.0E-1);
		entityLinkList[11].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[11].setFromEntity(entityList[7].getUri());
		entityLinkList[11].setLinkReason("");
		entityLinkList[11].setProvenance("InfoLink");
		entityLinkList[11].setToEntity(entityList[12].getUri());
		
		entityLinkList[12].setConfidence(9.0E-1);
		entityLinkList[12].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[12].setFromEntity(entityList[7].getUri());
		entityLinkList[12].setLinkReason("");
		entityLinkList[12].setProvenance("InfoLink");
		entityLinkList[12].setToEntity(entityList[13].getUri());

		entityLinkList[13].setConfidence(9.0E-1);
		entityLinkList[13].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[13].setFromEntity(entityList[7].getUri());
		entityLinkList[13].setLinkReason("");
		entityLinkList[13].setProvenance("InfoLink");
		entityLinkList[13].setToEntity(entityList[14].getUri());

		entityLinkList[14].setConfidence(9.0E-1);
		entityLinkList[14].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[14].setFromEntity(entityList[7].getUri());
		entityLinkList[14].setLinkReason("");
		entityLinkList[14].setProvenance("InfoLink");
		entityLinkList[14].setToEntity(entityList[15].getUri());

		entityLinkList[15].setConfidence(9.0E-1);
		entityLinkList[15].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[15].setFromEntity(entityList[7].getUri());
		entityLinkList[15].setLinkReason("");
		entityLinkList[15].setProvenance("InfoLink");
		entityLinkList[15].setToEntity(entityList[16].getUri());

		// *Kopie 12
		entityLinkList[16].setConfidence(9.0E-1);
		entityLinkList[16].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[16].setFromEntity(entityList[7].getUri());
		entityLinkList[16].setLinkReason("");
		entityLinkList[16].setProvenance("InfoLink");
		entityLinkList[16].setToEntity(entityList[12].getUri());

		entityLinkList[17].setConfidence(9.0E-1);
		entityLinkList[17].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[17].setFromEntity(entityList[7].getUri());
		entityLinkList[17].setLinkReason("");
		entityLinkList[17].setProvenance("InfoLink");
		entityLinkList[17].setToEntity(entityList[17].getUri());

		entityLinkList[18].setConfidence(9.0E-1);
		entityLinkList[18].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[18].setFromEntity(entityList[7].getUri());
		entityLinkList[18].setLinkReason("");
		entityLinkList[18].setProvenance("InfoLink");
		entityLinkList[18].setToEntity(entityList[18].getUri());

		entityLinkList[19].setConfidence(9.0E-1);
		entityLinkList[19].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[19].setFromEntity(entityList[7].getUri());
		entityLinkList[19].setLinkReason("");
		entityLinkList[19].setProvenance("InfoLink");
		entityLinkList[19].setToEntity(entityList[19].getUri());

		entityLinkList[20].setConfidence(9.0E-1);
		entityLinkList[20].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[20].setFromEntity(entityList[7].getUri());
		entityLinkList[20].setLinkReason("");
		entityLinkList[20].setProvenance("InfoLink");
		entityLinkList[20].setToEntity(entityList[20].getUri());

		entityLinkList[21].setConfidence(9.0E-1);
		entityLinkList[21].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[21].setFromEntity(entityList[7].getUri());
		entityLinkList[21].setLinkReason("");
		entityLinkList[21].setProvenance("InfoLink");
		entityLinkList[21].setToEntity(entityList[21].getUri());

		// *Kopie 20
		entityLinkList[22].setConfidence(9.0E-1);
		entityLinkList[22].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[22].setFromEntity(entityList[7].getUri());
		entityLinkList[22].setLinkReason("");
		entityLinkList[22].setProvenance("InfoLink");
		entityLinkList[22].setToEntity(entityList[20].getUri());
		
		entityLinkList[23].setConfidence(9.0E-1);
		entityLinkList[23].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[23].setFromEntity(entityList[7].getUri());
		entityLinkList[23].setLinkReason("");
		entityLinkList[23].setProvenance("InfoLink");
		entityLinkList[23].setToEntity(entityList[22].getUri());
		
		entityLinkList[24].setConfidence(9.0E-1);
		entityLinkList[24].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[24].setFromEntity(entityList[7].getUri());
		entityLinkList[24].setLinkReason("");
		entityLinkList[24].setProvenance("InfoLink");
		entityLinkList[24].setToEntity(entityList[23].getUri());
		
		// *Kopie 23
		entityLinkList[25].setConfidence(9.0E-1);
		entityLinkList[25].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[25].setFromEntity(entityList[7].getUri());
		entityLinkList[25].setLinkReason("");
		entityLinkList[25].setProvenance("InfoLink");
		entityLinkList[25].setToEntity(entityList[23].getUri());
		
		// *Kopie 22
		entityLinkList[26].setConfidence(9.0E-1);
		entityLinkList[26].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[26].setFromEntity(entityList[7].getUri());
		entityLinkList[26].setLinkReason("");
		entityLinkList[26].setProvenance("InfoLink");
		entityLinkList[26].setToEntity(entityList[22].getUri());
				
		entityLinkList[27].setConfidence(9.0E-1);
		entityLinkList[27].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[27].setFromEntity(entityList[7].getUri());
		entityLinkList[27].setLinkReason("");
		entityLinkList[27].setProvenance("InfoLink");
		entityLinkList[27].setToEntity(entityList[24].getUri());
		
		entityLinkList[28].setConfidence(9.0E-1);
		entityLinkList[28].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[28].setFromEntity(entityList[7].getUri());
		entityLinkList[28].setLinkReason("");
		entityLinkList[28].setProvenance("InfoLink");
		entityLinkList[28].setToEntity(entityList[25].getUri());
		
		entityLinkList[29].setConfidence(9.0E-1);
		entityLinkList[29].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[29].setFromEntity(entityList[7].getUri());
		entityLinkList[29].setLinkReason("");
		entityLinkList[29].setProvenance("InfoLink");
		entityLinkList[29].setToEntity(entityList[26].getUri());
		
		// *Kopie 19
		entityLinkList[30].setConfidence(9.0E-1);
		entityLinkList[30].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[30].setFromEntity(entityList[7].getUri());
		entityLinkList[30].setLinkReason("");
		entityLinkList[30].setProvenance("InfoLink");
		entityLinkList[30].setToEntity(entityList[19].getUri());
		
		entityLinkList[31].setConfidence(9.0E-1);
		entityLinkList[31].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[31].setFromEntity(entityList[7].getUri());
		entityLinkList[31].setLinkReason("");
		entityLinkList[31].setProvenance("InfoLink");
		entityLinkList[31].setToEntity(entityList[27].getUri());
		
		entityLinkList[32].setConfidence(9.0E-1);
		entityLinkList[32].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[32].setFromEntity(entityList[7].getUri());
		entityLinkList[32].setProvenance("InfoLink");
		entityLinkList[32].setLinkReason("");
		entityLinkList[32].setToEntity(entityList[28].getUri());
		
		entityLinkList[33].setConfidence(9.0E-1);
		entityLinkList[33].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[33].setFromEntity(entityList[7].getUri());
		entityLinkList[33].setLinkReason("");
		entityLinkList[33].setProvenance("InfoLink");
		entityLinkList[33].setToEntity(entityList[29].getUri());
		
		// *Kopie 25
		entityLinkList[34].setConfidence(9.0E-1);
		entityLinkList[34].setEntityRelations(new HashSet<>(Arrays.asList(EntityRelation.part_of_temporal))); 
		entityLinkList[34].setFromEntity(entityList[7].getUri());
		entityLinkList[34].setLinkReason("");
		entityLinkList[34].setProvenance("InfoLink");
		entityLinkList[34].setToEntity(entityList[25].getUri());
		
		dataStoreClient.post(EntityLink.class, Arrays.asList(entityLinkList));
		
		//List<String> links = Arrays.asList(entityLinkList[0].getUri(), entityLinkList[1].getUri(), entityLinkList[2].getUri(), entityLinkList[3].getUri(), entityLinkList[4].getUri(), entityLinkList[5].getUri());
		//exec.setLinks(links);
		
		// TODO tests
		LinkIndexer indexer = new LinkIndexer(dataStoreClient, dataStoreClient, fileResolver, fileResolver);
		indexer.setExecution(exec);
		//List<EntityLink> listetityLink = indexer.flattenLinks(Arrays.asList(entityLinkList));
		//org.junit.Assert.assertEquals(1,listetityLink.size());
		//org.junit.Assert.assertEquals(, listetityLink.get(0).getEntityType());
		
	}
	
	//TODO move below tests to different class and package
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
		//ArrayList<String> queryStrings = new ArrayList<String>();
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
