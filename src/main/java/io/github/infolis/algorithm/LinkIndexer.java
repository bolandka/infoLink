package io.github.infolis.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import java.io.StringReader;
import java.util.Arrays;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
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
		HttpClient httpclient = HttpClients.createDefault();
		HttpPost httppost = new HttpPost(dataSearchIndex);
		String answer = null;
		try {
			answer = post(httpclient, httppost, new StringEntity(query, ContentType.APPLICATION_JSON));
		} catch (IOException e) { log.error(e.toString()); }
		if (null == answer) return null;

		JsonObject json = Json.createReader(new StringReader(answer)).readObject();
		JsonObject hits1 = json.getJsonObject("hits");
		JsonArray hits = hits1.getJsonArray("hits");
		
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

	private List<EntityLink> getFlattenedLinksForEntity(
			Multimap<String, String> entityEntityMap, 
			Multimap<String, String> entitiesLinkMap,
			String startEntityUri, Multimap<String, String> toEntities, 
			List<EntityLink> processedLinks) {
		List<EntityLink> flattenedLinks = new ArrayList<>();
		for (Map.Entry<String, String> entry : toEntities.entries()) {
			Entity toEntity = getInputDataStoreClient().get(Entity.class, entry.getKey().replaceAll("http://.*/entity", "http://svkolodtest.gesis.intra/link-db/api/entity"));
			EntityLink link = getInputDataStoreClient().get(EntityLink.class, entry.getValue().replaceAll("http://.*/entityLink", "http://svkolodtest.gesis.intra/link-db/api/entityLink"));
			
			toEntities = ArrayListMultimap.create();
			for (String toEntityUri : entityEntityMap.get(toEntity.getUri())) {
				toEntities.putAll(toEntityUri, entitiesLinkMap.get(toEntity.getUri() + toEntityUri));
			}
			if ((!toEntity.getEntityType().equals(EntityType.citedData)) || toEntities.isEmpty()) {

				EntityLink directLink = new EntityLink();
				directLink.setFromEntity(startEntityUri);
				directLink.setToEntity(link.getToEntity());
				directLink.setEntityRelations(link.getEntityRelations());
				// set cited data as link view
				if (!toEntity.getEntityType().equals(EntityType.citedData)) {
					Entity fromEntity = getInputDataStoreClient().get(Entity.class, link.getFromEntity().replaceAll("http://.*/entity", "http://svkolodtest.gesis.intra/link-db/api/entity"));
					StringJoiner linkView = new StringJoiner(" ");
					//TODO check
					if (fromEntity.getEntityType().equals(EntityType.citedData)) {
						// hack for infolis links
						if (null == fromEntity.getEntityView() || fromEntity.getEntityView().isEmpty()) {
							linkView.add(fromEntity.getName());
							for (String number : fromEntity.getNumericInfo()) linkView.add(number);
							fromEntity.setEntityView(linkView.toString());
							getOutputDataStoreClient().put(Entity.class, fromEntity, fromEntity.getUri());
						}
					}
					directLink.setLinkView(fromEntity.getEntityView());
				} else {
					StringJoiner linkView = new StringJoiner(" ");

					// set link view for infolisLinks
					if (null == toEntity.getEntityView() || toEntity.getEntityView().isEmpty()) {
						linkView.add(toEntity.getName());
						if (null != toEntity.getNumericInfo() && !toEntity.getNumericInfo().isEmpty()) {
							for (String number : toEntity.getNumericInfo()) linkView.add(number);
						}
						toEntity.setEntityView(linkView.toString());
						getOutputDataStoreClient().put(Entity.class, toEntity, toEntity.getUri());
					} 
					directLink.setLinkView(toEntity.getEntityView());
				}
				directLink.setTags(link.getTags());
				
				int intermediateLinks = processedLinks.size();
				String linkReason = null;
				if (null != link.getLinkReason() && !link.getLinkReason().isEmpty()) linkReason = link.getLinkReason().replaceAll("http://.*/textualReference", "http://svkolodtest.gesis.intra/link-db/api/textualReference");
				double confidenceSum = 0;
				Set<String> provenance = new HashSet<>();
				for (EntityLink intermediateLink : processedLinks) {
					confidenceSum += intermediateLink.getConfidence();
					if (null != intermediateLink.getLinkReason()) {
						linkReason = intermediateLink.getLinkReason().replaceAll("http://.*/textualReference", "http://svkolodtest.gesis.intra/link-db/api/textualReference");
					}
					directLink.addAllTags(intermediateLink.getTags());
					for (EntityRelation relation : intermediateLink.getEntityRelations()) {
						if (!relation.equals(EntityRelation.same_as)) directLink.addEntityRelation(relation);
					}
					// provenance entries of intermediate links do not have to be equal - e.g. manually specified cited data may have been linked to datasets automatically
					if (null != intermediateLink.getProvenance() && !intermediateLink.getProvenance().isEmpty()) provenance.add(intermediateLink.getProvenance());
				}
				confidenceSum += link.getConfidence();
				intermediateLinks += 1;
				directLink.setConfidence(confidenceSum / intermediateLinks);
				directLink.setLinkReason(linkReason);
				log.debug("reference: " + linkReason);
						
				if (null != link.getProvenance() && !link.getProvenance().isEmpty()) provenance.add(link.getProvenance());
				if (null == provenance || provenance.isEmpty()) directLink.setProvenance("InfoLink");
				else directLink.setProvenance(String.join(" + ", provenance));
						
				directLink.addAllTags(getExecution().getTags());
				log.debug("flattenedLink: " + SerializationUtils.toJSON(directLink));
				flattenedLinks.add(directLink);

			} else {
				processedLinks.add(link);
				flattenedLinks.addAll(getFlattenedLinksForEntity(entityEntityMap,
					entitiesLinkMap, startEntityUri,
					toEntities,
					processedLinks));
			}
		}
		return flattenedLinks;
	}
	
	private List<EntityLink> flattenLinks(List<EntityLink> links) {
		List<EntityLink> flattenedLinks = new ArrayList<>();
		Multimap<String, String> entityEntityMap = ArrayListMultimap.create();
		Multimap<String, String> entitiesLinkMap = ArrayListMultimap.create();
		for (EntityLink link : links) {
			Entity fromEntity = getInputDataStoreClient().get(Entity.class, link.getFromEntity().replaceAll("http://.*/entity", "http://svkolodtest.gesis.intra/link-db/api/entity"));
			if (fromEntity.getTags().contains("infolis-ontology")) continue;
			entityEntityMap.put(fromEntity.getUri(), link.getToEntity());
			entitiesLinkMap.put(fromEntity.getUri()+link.getToEntity(), link.getUri());
		}
		
		for (String entityUri : entityEntityMap.keySet()) {
			Entity fromEntity = getInputDataStoreClient().get(Entity.class, entityUri.replaceAll("http://.*/entity", "http://svkolodtest.gesis.intra/link-db/api/entity"));
			Multimap<String, String> linkedEntities = ArrayListMultimap.create();
			if (fromEntity.getEntityType().equals(EntityType.citedData)) continue;
			for (String toEntityUri : entityEntityMap.get(entityUri)) {
				linkedEntities.putAll(toEntityUri, entitiesLinkMap.get(entityUri + toEntityUri));
			}
			
			flattenedLinks.addAll(getFlattenedLinksForEntity(entityEntityMap, entitiesLinkMap, entityUri, linkedEntities, new ArrayList<>()));
		}
		return flattenedLinks;
	}
	
	void pushToIndex(String index, List<EntityLink> flattenedLinks) throws ClientProtocolException, IOException {
		Set<Entity> entities = new HashSet<>();	
		String prefixRegex = "http://.*/entity/";
		if (null == index || index.isEmpty()) index = InfolisConfig.getElasticSearchIndex();
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
						fromEntity.setGwsId(pubId);
						break;
					}
				}
				if (null != fromEntity.getYear() && !fromEntity.getYear().isEmpty()) {;}
				else if (null!= fromEntity.getNumericInfo() && !fromEntity.getNumericInfo().isEmpty()) fromEntity.setYear(fromEntity.getNumericInfo().get(0));
				else fromEntity.setYear("o.J.");
				
				if (null!= toEntity.getNumericInfo() && !toEntity.getNumericInfo().isEmpty()) toEntity.setYear(toEntity.getNumericInfo().get(0));
				else fromEntity.setYear("o.J.");
				
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
			// set URI in a way that duplicates are overriden
			//TODO do not overwrite links; instead:search if links with such a uri already exist. if so: merge linkReasons and confidence scores, pssibly other fields as well?
			elink.setUri(elink.getGws_fromID() + "---" + elink.getGws_toID());
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
