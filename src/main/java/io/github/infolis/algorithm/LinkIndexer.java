package io.github.infolis.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.Arrays;

import java.io.StringReader;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonNumber;
import com.google.common.collect.Multimap;
import com.google.common.collect.HashMultimap;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;

import io.github.infolis.InfolisConfig;
import io.github.infolis.datastore.DataStoreClient;
import io.github.infolis.datastore.FileResolver;
import io.github.infolis.model.EntityType;
import io.github.infolis.model.entity.Entity;
import io.github.infolis.model.entity.EntityLink;
import io.github.infolis.model.entity.EntityLink.EntityRelation;
import io.github.infolis.util.SerializationUtils;

public class LinkIndexer extends ElasticIndexer {

	private static final Logger log = LoggerFactory.getLogger(LinkIndexer.class);

	public LinkIndexer(DataStoreClient inputDataStoreClient, DataStoreClient outputDataStoreClient,
			FileResolver inputFileResolver, FileResolver outputFileResolver) {
		super(inputDataStoreClient, outputDataStoreClient, inputFileResolver, outputFileResolver);
	}
	
	//TODO taken from dataProcessing's Importer..refactor
	private Entity getUniqueEntity(Multimap<String, String> toSearch) {
		List<Entity> hits = getInputDataStoreClient().search(Entity.class, toSearch);
		if (hits.size() > 1) log.warn("warning: found more than one entity " + toSearch.toString());
		if (hits.size() < 1) return null;
		else {
			log.debug("Found entity " + toSearch.toString());
			return hits.get(0);
		}
	}
		
	private Entity getDataSearchEntity(String doi) {
		String doiPrefix = "http://dx.doi.org/";
		doi = doi.replace(doiPrefix, "");
		//search in local database: datasearch doi found? if so return, if not:
			//connect to datasearch es index
			//search doi, create entity with all needed metadata, push to datastore and return
		Entity dataset = getDatasetWithDoi(doi);
		if (null == dataset) dataset = getDatasearchDatasetWithDoi(doi);
		return dataset;
	}

	private JsonArray getHitsFromESIndex(String index, String query) {
		HttpClient httpclient = HttpClients.createDefault();
		HttpPost httppost = new HttpPost(index);
		String answer = null;
		try {
			answer = post(httpclient, httppost, new StringEntity(query, ContentType.APPLICATION_JSON));
		} catch (IOException e) { log.error(e.toString()); }
		if (null == answer) return null;

		log.debug(answer);
		JsonObject json = Json.createReader(new StringReader(answer)).readObject();
		JsonObject hits1 = json.getJsonObject("hits");
		try {
			JsonArray hits = hits1.getJsonArray("hits");
			return hits;
		} catch (NullPointerException npe) {
			log.warn(npe.toString());
			log.debug(json.toString());
		}
		return null;
	}

	protected JsonObject getJsonFromESIndex(String index, String query) {
		HttpClient httpclient = HttpClients.createDefault();
		HttpPost httppost = new HttpPost(index);
		String answer = null;
		try {
			answer = post(httpclient, httppost, new StringEntity(query, ContentType.APPLICATION_JSON));
		} catch (IOException e) { log.error(e.toString()); }
		if (null == answer) return null;

		log.trace(answer);
		return Json.createReader(new StringReader(answer)).readObject();
	}

	private List<String> toJavaStringList(JsonArray jsonArray) {
		List<String> list = new ArrayList<>();
		for (int i=0; i<jsonArray.size(); i++) {
			list.add(jsonArray.getString(i));
		}
		return list;
	}

	private Set<EntityRelation> toJavaEntityRelationSet(JsonArray jsonArray) {
		Set<EntityRelation> set = new HashSet<>();
		for (int i=0; i<jsonArray.size(); i++) {
			set.add(EntityRelation.valueOf(jsonArray.getString(i)));
		}
		return set;
	}
	private ElasticLink getLinkFromIndex(String index, String id) {
		String query = String.format("{ \"query\": {" +
			"\"bool\": {" +
				"\"must\": [{" + 
					"\"query_string\": {"+
					"\"default_field\": \"_all\"," +
					"\"query\": \"_id:%s\"" +
					"}" +
				"}]," +
				"\"must_not\":[]," +
				"\"should\":[]" +
			"}" +
			"}," +
			"\"from\":0," +
			"\"size\":10," + 
			"\"sort\":[]," + 
			"\"facets\":{}" + 
			"}"
			, id);
		JsonArray hits = getHitsFromESIndex(index, query);
		if (null == hits || hits.isEmpty()) return null;
		log.debug(hits.toString());

		JsonObject entry = (JsonObject) hits.get(0);
		JsonObject metadata = (JsonObject) entry.get("_source");
		//JsonObject metadata = (JsonObject) source.get("dc");
		JsonArray linkReasons = (JsonArray) metadata.get("gws_linkReasons");
		String linkReason = metadata.getString("linkReason");
		String fromEntity = metadata.getString("fromEntity");
		String toEntity = metadata.getString("toEntity");
		String gws_fromID = metadata.getString("gws_fromID");
		String gws_toID = metadata.getString("gws_toID");
		String linkView = metadata.getString("linkView");
		String gws_link = null;
		try {
			gws_link = metadata.getString("gws_link");
		} catch (ClassCastException cce) {;}
		JsonNumber _confidence = metadata.getJsonNumber("confidence");
		double confidence = _confidence.doubleValue();
		JsonArray entityRelations = (JsonArray) metadata.get("entityRelations");
		EntityType gws_fromType = EntityType.valueOf(metadata.getString("gws_fromType"));
		EntityType gws_toType = EntityType.valueOf(metadata.getString("gws_toType"));
		String gws_fromView = metadata.getString("gws_fromView");
		String gws_toView = metadata.getString("gws_toView");

		ElasticLink link = new ElasticLink();
		link.setFromEntity(fromEntity);
		link.setToEntity(toEntity);
		List<String> _linkReasons = new ArrayList<>();
		link.setGws_linkReasons(toJavaStringList(linkReasons));
		link.setLinkReason(linkReason);
		link.setGws_fromID(gws_fromID);
		link.setGws_toID(gws_toID);
		link.setGws_fromType(gws_fromType);
		link.setGws_toType(gws_toType);
		link.setGws_toView(gws_toView);
		link.setGws_fromView(gws_fromView);
		link.setLinkView(linkView);
		link.setGws_link(gws_link);
		link.setConfidence(confidence);	
		link.setEntityRelations(toJavaEntityRelationSet(entityRelations));
		return link;
	}

	private Entity getDatasearchDatasetWithDoi(String doi) {
		String dataSearchIndex = "http://193.175.238.35:8089/dc/_search";
		String query = "{ \"query\": {" +
					"\"bool\": { " +
						"\"must\": [ " + 
							"{\"nested\": {" +
								"\"path\": \"dc.identifier\"," +
									"\"query\": {" +
										"\"bool\": {" +
											"\"must\": [" +
											"{" +
												"\"match\": {" +
													"\"dc.identifier.nn\": \"" + doi + "\"" +
												"}" +
											"}" +
											"]" +
										"}" +
									"}" +
								"}" +
							"}" +
						"]" +
					"}" +
				"}" +
			"}";
	
		JsonArray hits = getHitsFromESIndex(dataSearchIndex, query);	
		if (hits.size() > 1) log.warn("Dataset with doi seems to be registered in more than one repository; using first definition found: " + doi);
		if (hits.size() < 1) return null;//log.debug(hits.get(0).toString());
		JsonObject entry = (JsonObject) hits.get(0);
		JsonObject source = (JsonObject) entry.get("_source");
		JsonObject metadata = (JsonObject) source.get("dc");
		//-> dc -> creator -> all // title -> all // date -> all OR anydateYear
		//log.debug(metadata.toString());
		JsonObject creatorLists = (JsonObject) metadata.get("creator");
		JsonArray creatorArray = (JsonArray) creatorLists.get("all");
		List<String> creatorList = new ArrayList<>();
		for (int i = 0; i<creatorArray.size(); i++) if(!creatorList.contains(creatorArray.getString(i))) creatorList.add(creatorArray.getString(i));
		
		JsonObject titleLists = (JsonObject) metadata.get("title");
		JsonArray titles = (JsonArray) titleLists.get("all");
		String title = null;
		// TODO different languages...
		if (null != titles) {
			if (titles.size() >= 1) title = titles.getString(0);
		}

		JsonArray yearArray = (JsonArray) metadata.get("anydateYear");
		String year = "o.J.";
		//TODO array to list; use first entry or, if more than one: don't use any...
		
		if (null != yearArray) {
			if (yearArray.size() > 1) log.debug("Warning: more than one entry in field anydateYear. Ignoring: " + yearArray.toString());
			else if (yearArray.size() == 1) year = yearArray.getString(0);
			else log.debug("No entry in field anydateYear");
		}

		Entity entity = new Entity();
		entity.setEntityType(EntityType.dataset);
		entity.setEntityProvenance("dataSearch");
		entity.setDoi(doi);
		String dataSearchId = source.get("esid").toString().replace("\"", "");
		String gwsId = "datasearch-" + dataSearchId.replaceAll("[.:/]", "-");
		entity.setIdentifiers(Arrays.asList(doi, dataSearchId, gwsId));
		entity.setGwsId(gwsId);
		if (null != title) entity.setName(title);
		entity.setYear(year);
		if (!creatorList.isEmpty()) entity.setAuthors(creatorList);
		entity = setEntityView(entity);	

		getOutputDataStoreClient().post(Entity.class, entity);
		return entity;
	}

	protected Entity setEntityView(Entity entity) {
		String year = entity.getYear();
		if (null == year) year = "o.J.";
		String authors = getAuthorString(entity.getAuthors());
		if (authors.isEmpty()) authors = getAuthorString(entity.getEditors());
		entity.setEntityView(String.format("%s (%s): %s", authors, year, entity.getName()));
		return entity;
	}


	public Entity getDatasetWithDoi(String doi) {
		Multimap<String, String> toSearch = HashMultimap.create();
		toSearch.put("identifiers", doi);
		//toSearch.put("doi", doi);
		toSearch.put("entityType", "dataset");
		toSearch.put("entityProvenance", "dbk");
		//toSearch.put("entityProvenance", "datorium");
		return getUniqueEntity(toSearch);
	}

	//modified
	protected String getAuthorString(List<String> authors) {
		StringJoiner author = new StringJoiner("; ");
		String authorString = null;
		int n = 0;
		// shorten and add "et al." when more than 2 authors
		if (null != authors) {
			for (String a : authors) {
				if (n > 1) {
					authorString = author.toString() + " et al.";
					break;
				} else {
					author.add(a);
					n++;
				}
			}	
		}
		if (null == authorString) return author.toString();
		else return authorString;
	}
	
	// end of importer's methods

	private <T extends Object> Collection<T> addIfExists(Collection<T> collection, T val) {
		if (null != val) {
			if (val instanceof String) 
				if (!String.valueOf(val).isEmpty()) collection.add(val);
			else collection.add(val);
		}
		return collection;
	}
		
	private EntityLink addAllMetadata(EntityLink link, List<EntityLink> processedLinks,
		List<Entity> processedEntities) {
	
		Set<String> linkReason_all = new HashSet<>();
		Set<Double> confidence_all = new HashSet<>();
		Set<EntityRelation> entityRelations_all = new HashSet<>();
		Set<String> provenance_all = new HashSet<>();
		Set<String> tags_all = new HashSet<>();
		List<String> linkView_all = new ArrayList<>();

		for (EntityLink intermediateLink : processedLinks) {
			addIfExists(linkReason_all, intermediateLink.getLinkReason());
			addIfExists(confidence_all, intermediateLink.getConfidence());
			addIfExists(provenance_all, intermediateLink.getProvenance());
			for (EntityRelation rel : intermediateLink.getEntityRelations()) 
				addIfExists(entityRelations_all, rel);
			for (String tag : intermediateLink.getTags()) 
				addIfExists(tags_all, tag);
		}
		for (Entity entity : processedEntities) {
			if (EntityType.citedData.equals(entity.getEntityType())) {
				// hack for infolis links
				if (null == entity.getEntityView() || entity.getEntityView().isEmpty()) {
					StringJoiner linkView = new StringJoiner(" ");
					linkView.add(entity.getName());
					for (String number : entity.getNumericInfo()) linkView.add(number);
					entity.setEntityView(linkView.toString());
					getOutputDataStoreClient().put(Entity.class, entity, entity.getUri());
				}
				linkView_all.add(entity.getEntityView());
			}
			for (String tag : entity.getTags()) 
				addIfExists(tags_all, tag);
		}

		link.setLinkReason(linkReason_all
				.stream()
				.collect(Collectors.joining("@@@")));
		link.setConfidence((confidence_all
				.stream()
				.mapToDouble(x->x)
				.sum()) / processedLinks.size());
		//if (!relation.equals(EntityRelation.same_as)) directLink.addEntityRelation(relation);
		link.setEntityRelations(entityRelations_all);
		link.setProvenance(provenance_all
				.stream()
				.collect(Collectors.joining(" + ")));
		link.addAllTags(tags_all);
		link.addAllTags(getExecution().getTags());
		// if the data is allowed to contain more than one intermediate cited data entity, this behaviour might need to be changed
		if (!linkView_all.isEmpty()) link.setLinkView(linkView_all.get(0));
		return link;
	}
			

	private List<EntityLink> getFlattenedLinksForEntity(
			Multimap<String, String> entityEntityMap,
			Multimap<String, String> entitiesLinkMap,
			String startEntityUri, String currentEntityUri, 
			List<EntityLink> processedLinks, List<Entity> processedEntities) {
			List<EntityLink> flattenedLinks = new ArrayList<>();
			Collection<String> connectedEntities = entityEntityMap.get(currentEntityUri);
			// currentEntity is the last entity in the chain
			if (null == connectedEntities || connectedEntities.isEmpty()) {
				if (startEntityUri.equals(currentEntityUri)) ;
				else {
					//create direct link
					EntityLink directLink = new EntityLink();
					directLink.setFromEntity(startEntityUri);
					directLink.setToEntity(currentEntityUri);
					//combine information of all intermediate links and entities
					directLink = addAllMetadata(directLink, processedLinks, processedEntities);

					flattenedLinks.add(directLink);
				}
				
			} else {
				for (String connectedEntityUri : connectedEntities) {
					// note: this may be more than one!
					// if so, they need to be merged...
					// get links connecting the two entities
					Collection<String> connectingLinksUris = entitiesLinkMap.get(currentEntityUri + connectedEntityUri);

					for (String currentLinkUri : connectingLinksUris) {
						EntityLink currentLink = getInputDataStoreClient().get(EntityLink.class, currentLinkUri);
						processedLinks.add(currentLink);
					}
					processedEntities.add(getInputDataStoreClient().get(Entity.class, currentEntityUri));

					flattenedLinks.addAll(getFlattenedLinksForEntity(
					entityEntityMap,
					entitiesLinkMap,
					startEntityUri, connectedEntityUri,
					processedLinks, processedEntities));
				}
			}
		return flattenedLinks;
	}
					

	protected List<EntityLink> flattenLinks(List<EntityLink> links) {
		List<EntityLink> flattenedLinks = new ArrayList<>();
		Multimap<String, String> entityEntityMap = ArrayListMultimap.create();
		Multimap<String, String> entitiesLinkMap = ArrayListMultimap.create();
		for (EntityLink link : links) {
			Entity fromEntity = getInputDataStoreClient().get(Entity.class, link.getFromEntity());
				
			if (fromEntity.getTags().contains("infolis-ontology")) continue;
			entityEntityMap.put(fromEntity.getUri(), link.getToEntity());
			entitiesLinkMap.put(fromEntity.getUri()+link.getToEntity(), link.getUri());
		}
		
		for (String entityUri : entityEntityMap.keySet()) {
			Entity fromEntity = getInputDataStoreClient().get(Entity.class, entityUri);
			// need to find the start entities of a link chain, do not call on start entities of intermediate links!
			// IMPORTANT: if new intermediate entity types are added, they need to be included here
			if (fromEntity.getEntityType().equals(EntityType.citedData)) continue;
			flattenedLinks.addAll(getFlattenedLinksForEntity(entityEntityMap, entitiesLinkMap, entityUri, entityUri, new ArrayList<>(), new ArrayList<>()));
		}
		return flattenedLinks;
	}


	// TODO implement full link merging including ontology stuff
	private ElasticLink mergeLinks(ElasticLink link1, ElasticLink link2) {
		// use highest confidence value
		if (link1.getConfidence() > link2.getConfidence()) link1.setConfidence(link2.getConfidence());
		// merge all linkReasons
		Set<String> linkReasons = new HashSet<>();
		linkReasons.addAll(link1.getGws_linkReasons());
		linkReasons.addAll(link2.getGws_linkReasons());
		link1.setGws_linkReasons(new ArrayList<>(linkReasons));
		// TODO what to do with entity relations and other fields?
		return link1;
	}
	
	void pushToIndex(String index, List<EntityLink> flattenedLinks) throws ClientProtocolException, IOException {
		Set<Entity> entities = new HashSet<>();	
		String prefixRegex = "http://.*/entity/";
		if (null == index || index.isEmpty()) {
			index = InfolisConfig.getElasticSearchIndex();
		}
		getExecution().setIndexDirectory(index);
		HttpClient httpclient = HttpClients.createDefault();
		
		for (EntityLink link : flattenedLinks) {
			Entity fromEntity = getInputDataStoreClient().get(Entity.class, link.getFromEntity().replaceAll(prefixRegex, "http://svkolodtest.gesis.intra/link-db/api/entity/"));
			Entity toEntity = getInputDataStoreClient().get(Entity.class, link.getToEntity().replaceAll(prefixRegex, "http://svkolodtest.gesis.intra/link-db/api/entity/"));
			// link to za numbers / elastic search entities
			log.debug(toEntity.getName());
			log.debug(toEntity.getEntityView());
			//quick hack for current ssoar infolink data; remove later
			//treat only infolis links here
			if (null == link.getProvenance() || link.getProvenance().isEmpty() || link.getProvenance().equals("InfoLink")) {
				if (toEntity.getEntityType().equals(EntityType.dataset)) {
					log.debug(toEntity.getIdentifiers().get(0));
					toEntity = getDataSearchEntity(toEntity.getIdentifiers().get(0));
					//log.debug(toEntity.getIdentifiers().get(0));
					log.debug(SerializationUtils.toJSON(toEntity).toString());
					//System.exit(0);
					//toEntity.setGwsId("doi:" + toEntity.getIdentifiers().get(0).replace("/", "-"));
					// ignore link if dataset wasn't found in dbk or elasticsearch
					if (null == toEntity) continue;
				}
	
				for (String pubId : fromEntity.getIdentifiers())  {
					if (null == pubId) continue;
					else if (pubId.startsWith("urn:")) {
						//fromEntity.setGwsId(pubId);
						String gwsId = pubId.replace("urn:nbn:de:0168-ssoar", "gesis-ssoar");
						gwsId = gwsId.substring(0, gwsId.length()-1);
						if (gwsId.endsWith("-")) gwsId = gwsId.substring(0, gwsId.length()-1);
						fromEntity.setGwsId(gwsId);
						fromEntity.getIdentifiers().add(gwsId);
						break;
					}
				}
				if (null != fromEntity.getYear() && !fromEntity.getYear().isEmpty()) {;}
				else if (null!= fromEntity.getNumericInfo() && !fromEntity.getNumericInfo().isEmpty()) fromEntity.setYear(fromEntity.getNumericInfo().get(0));
				else fromEntity.setYear("o.J.");
				
				if (null!= toEntity.getNumericInfo() && !toEntity.getNumericInfo().isEmpty()) toEntity.setYear(toEntity.getNumericInfo().get(0));
				else toEntity.setYear("o.J.");
				
				StringJoiner fromEntityAuthor = new StringJoiner("; ");
				if (null != fromEntity.getAuthors()) for (String author : fromEntity.getAuthors()) fromEntityAuthor.add(author);

				fromEntity.setEntityView(String.format("%s (%s): %s", fromEntityAuthor.toString(), fromEntity.getYear(), fromEntity.getName()));
			}
			
			if (null != fromEntity.getGwsId()) fromEntity.setUri(fromEntity.getGwsId());
			else fromEntity.setUri(fromEntity.getUri().replaceAll("http.*/entity/","literaturpool-"));
			if (null != toEntity.getGwsId()) toEntity.setUri(toEntity.getGwsId());
			else toEntity.setUri(toEntity.getUri().replaceAll("http.*/entity/","literaturpool-"));

			// TODO dedup entities:
			// check if similar entity is in temp data store (or other data store?)
			// if so, merge entities and merge links
			// for this, set up ingoingLinks and outgoingLinks maps
			//   containing entities as keys and in-/outgoing links as values
			// in those links, replace ID of entity with new merged ID

			link.setFromEntity(fromEntity.getUri());
			link.setToEntity(toEntity.getUri());

			//don't push link to index if its entities have too little metadata
			if (!showInGws(fromEntity, toEntity)) continue;
			ElasticLink elink = new ElasticLink(link);
			elink.setGws_fromView(fromEntity.getEntityView());
			elink.setGws_toView(toEntity.getEntityView());
			elink.setGws_fromType(fromEntity.getEntityType());
			elink.setGws_toType(toEntity.getEntityType());
			List<String> _linkReasons = Arrays.stream(elink.getLinkReason().split("@@@"))
				.filter(x -> (x != null && !x.isEmpty()))
				.collect(Collectors.toList());
			elink.setAndResolveGws_linkReasons(_linkReasons);
			// set URI in a way that duplicates are overriden
			//TODO do not overwrite links; instead:search if links with such a uri already exist. if so: merge linkReasons and confidence scores, pssibly other fields as well?
			String linkUri = elink.getGws_fromID() + "---" + elink.getGws_toID();
			ElasticLink duplicate = getLinkFromIndex(getExecution().getIndexDirectory() + "_search", linkUri);
			if (null != duplicate) elink = mergeLinks(elink, duplicate);

			elink.setUri(linkUri);

			if (toEntity.getEntityType().equals(EntityType.citedData)) elink.setGws_link(elink.getGwsLink(toEntity.getName().replaceAll("\\d", "").trim()));
			if (null != elink.getUri()) {
				HttpPut httpput = new HttpPut(index + "EntityLink/" + elink.getUri().replaceAll("http://.*/entityLink/", ""));
				put(httpclient, httpput, new StringEntity(SerializationUtils.toJSON(elink), ContentType.APPLICATION_JSON));
				log.debug(String.format("put link \"%s\" to %s", elink, index));
			}
			// flattened links are not pushed to any datastore and thus have no uri
			else {
				HttpPost httppost = new HttpPost(index + "EntityLink/");
				post(httpclient, httppost, new StringEntity(SerializationUtils.toJSON(elink), ContentType.APPLICATION_JSON));
				log.debug(String.format("posted link \"%s\" to %s", elink, index));
			}
				entities.add(fromEntity);
				entities.add(toEntity);
		}

		for (Entity entity : entities) {
			HttpPut httpput = new HttpPut(index + "Entity/" + entity.getUri());
			put(httpclient, httpput, new StringEntity(SerializationUtils.toJSON(entity), ContentType.APPLICATION_JSON));
			log.debug(String.format("put entity \"%s\" to %s", entity.getUri(), index));
		}
		
	}

	List<EntityLink> getLinksToPush() {
		return flattenLinks(getInputDataStoreClient().get(EntityLink.class, getExecution().getLinks()));
	}

}
