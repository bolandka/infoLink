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
import io.github.infolis.model.TextualReference;
import io.github.infolis.model.entity.Entity;
import io.github.infolis.model.entity.EntityLink;
import io.github.infolis.model.entity.EntityLink.EntityRelation;
import io.github.infolis.util.SerializationUtils;

public class LinkIndexer extends ElasticIndexer {

	private static final Logger log = LoggerFactory.getLogger(LinkIndexer.class);
	protected List<ElasticLink> elinks = new ArrayList<>();
	protected Set<Entity> entities = new HashSet<>();	
	private String apiEntityPrefixRegex = "http://.*/entity/";
	private String apiEntityLinkPrefixRegex = "http://.*/entityLink/";
	private String apiTextualReferencePrefixRegex = "http://.*/textualReference/";
	private String apiEntityPrefixReplacement = InfolisConfig.getFrontendURI() + "/entity/";
	private String apiEntityLinkPrefixReplacement = InfolisConfig.getFrontendURI() + "/entityLink/";
	private String apiTextualReferencePrefixReplacement = InfolisConfig.getFrontendURI() + "/textualReference/";
	
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
		String doiPrefixOld = "http://dx.doi.org/";
		String doiPrefixNew = "https://www.doi.org/";
		String doiPrefixNew2 = "https://doi.org/";
		doi = doi.replace(doiPrefixOld, "");
		doi = doi.replace(doiPrefixNew, "");
		doi = doi.replace(doiPrefixNew2, "");
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
		// error is thrown when searching the first links in the index which naturally does not exist yet
		//} catch (IndexMissingException ime) {;}
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
		} catch (IOException e) { log.error(e.toString()); 
		}
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
			"\"size\":1," + 
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
		String linkView = null;
		// value may be null -> null cannot be cast to string
		try {
			linkView = metadata.getString("linkView");
		} catch (ClassCastException cce) {;}
		String gws_link = null;
		try {
			gws_link = metadata.getString("gws_link");
		} catch (ClassCastException cce) {;}
		double confidence = Double.NaN;
		try {
			String _confidence = metadata.getString("confidence");
			if (null != _confidence && !_confidence.isEmpty())
			confidence = Double.parseDouble(_confidence);
		} catch (ClassCastException cce) {
			try {
				JsonNumber __confidence = metadata.getJsonNumber("confidence");
				confidence = __confidence.doubleValue();
			} catch (ClassCastException e) {;}
		}
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
		if (Double.NaN != confidence) link.setConfidence(confidence);	
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
		if (val instanceof Double) {
			if (Double.NaN != (Double)val) collection.add(val);
		}
		else if (null != val) {
			if (val instanceof String) 
				if (!(String.valueOf(val).isEmpty())) collection.add(val);
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
			addIfExists(linkView_all, intermediateLink.getLinkView());
			/*for (EntityRelation rel : intermediateLink.getEntityRelations()) 
				addIfExists(entityRelations_all, rel);*/
			entityRelations_all.addAll(intermediateLink.getEntityRelations());
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
		if (!confidence_all.isEmpty()) 
			link.setConfidence(((confidence_all
				.stream()
				//.mapToDouble(x->x)
				//.sum())) / confidence_all.size());
				.collect(Collectors.summingDouble(d->d))) / confidence_all.size()));
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
	
	// if other immediate entity types are added, add them here
	private boolean finalEntitiesOnly(Collection<String> entities) {
		for (Entity entity : getInputDataStoreClient().get(Entity.class, entities)) {
			if (EntityType.citedData.equals(entity.getEntityType())) return false;
		}
		return true;
	}

	private boolean finalEntitiesOnly(String entity) {
		if (EntityType.citedData.equals(getInputDataStoreClient().get(Entity.class, entity).getEntityType())) return false;
		else return true;
	}

	private List<EntityLink> getFlattenedLinksForEntity(
			Multimap<String, String> entityEntityMap,
			Multimap<String, String> entitiesLinkMap,
			String startEntityUri, String currentEntityUri, 
			List<EntityLink> processedLinks, List<Entity> processedEntities) {
			List<EntityLink> flattenedLinks = new ArrayList<>();
			Collection<String> connectedEntities = entityEntityMap.get(currentEntityUri);
			// currentEntity is the last entity in the chain
			if (null == connectedEntities || connectedEntities.isEmpty() || ((finalEntitiesOnly(connectedEntities))  && finalEntitiesOnly(currentEntityUri))) {
				// process direct links that need no flattening...
				if (startEntityUri.equals(currentEntityUri) && null != connectedEntities && !connectedEntities.isEmpty())  {
					processedEntities.add(getInputDataStoreClient().get(Entity.class, currentEntityUri));
					for (String connectedEntityUri : connectedEntities) {
						for (String outgoingLinkUri : entitiesLinkMap.get(
							startEntityUri + connectedEntityUri)) {
							EntityLink outgoingLink = getInputDataStoreClient().get(
								EntityLink.class, outgoingLinkUri);
								processedLinks.add(outgoingLink);
						}
						flattenedLinks.addAll(getFlattenedLinksForEntity(
							entityEntityMap,
							entitiesLinkMap,
							startEntityUri,
							connectedEntityUri,
							processedLinks,
							processedEntities));
					}	
				}
				else {
					//create direct link
					EntityLink directLink = new EntityLink();
					directLink.setFromEntity(startEntityUri);
					directLink.setToEntity(currentEntityUri);
					processedEntities.add(getInputDataStoreClient().get(Entity.class, currentEntityUri));
					//combine information of all intermediate links and entities
					directLink = addAllMetadata(directLink, processedLinks, processedEntities);
					flattenedLinks.add(directLink);
					processedLinks = new ArrayList<>();
					processedEntities = new ArrayList<>();
				}
				
			} else {
				for (String connectedEntityUri : connectedEntities) {
					// note: this may be more than one!
					// if so, they need to be merged...
					// get links connecting the two entities
					Collection<String> connectingLinksUris = entitiesLinkMap.get(currentEntityUri + connectedEntityUri);

					List<EntityLink> newProcessedLinks = new ArrayList<>(processedLinks);
					List<Entity> newProcessedEntities = new ArrayList<>(processedEntities);

					for (String currentLinkUri : connectingLinksUris) {
						EntityLink currentLink = getInputDataStoreClient().get(EntityLink.class, currentLinkUri);
						newProcessedLinks.add(currentLink);
					}
					
					newProcessedEntities.add(getInputDataStoreClient().get(Entity.class, currentEntityUri));

					flattenedLinks.addAll(getFlattenedLinksForEntity(
					entityEntityMap,
					entitiesLinkMap,
					startEntityUri, connectedEntityUri,
					newProcessedLinks, newProcessedEntities));
					//newProcessedLinks = new ArrayList<>();
					//newProcessedEntities = new ArrayList<>();
				}
			}
		return flattenedLinks;
	}
					

	protected List<EntityLink> flattenLinks(List<EntityLink> links) {
		List<EntityLink> flattenedLinks = new ArrayList<>();
		Multimap<String, String> entityEntityMap = ArrayListMultimap.create();
		Multimap<String, String> entitiesLinkMap = ArrayListMultimap.create();
		for (EntityLink link : links) {
			Entity fromEntity = getInputDataStoreClient().get(Entity.class, link.getFromEntity().replaceAll(apiEntityPrefixRegex, apiEntityPrefixReplacement));
				
			if (fromEntity.getTags().contains("infolis-ontology")) continue;
			entityEntityMap.put(fromEntity.getUri(), link.getToEntity().replaceAll(apiEntityPrefixRegex, apiEntityPrefixReplacement));
			entitiesLinkMap.put(fromEntity.getUri()+link.getToEntity().replaceAll(apiEntityPrefixRegex, apiEntityPrefixReplacement), link.getUri().replaceAll(apiEntityLinkPrefixRegex, apiEntityLinkPrefixReplacement));
		}
		
		for (String entityUri : entityEntityMap.keySet()) {
			Entity fromEntity = getInputDataStoreClient().get(Entity.class, entityUri.replaceAll(apiEntityPrefixRegex, apiEntityPrefixReplacement));
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
		if (link1.getConfidence() < link2.getConfidence()) link1.setConfidence(link2.getConfidence());

		// below code causes the algorithm to also merge e.g. the link reasons of 
		//  ALLBUS 2000 and of ALLBUS 2000-2010
		// They might have the same link reason and point to the same dataset
		// But the inserted marker **[]** would be at different positions and 
		// the resulting snippets would look like weird duplicates...
		// Thus: do not merge the field linkReasons, merge linkReason instead 
		// and resolve...
		// merge all linkReasons
		Set<String> linkReasons = new HashSet<>();
		Set<String> linkReasonsWithoutMarkup = 
			link1.getGws_linkReasons()
			.stream()
			.map(x -> x.replaceAll("\\*\\*\\[\\s*","").replaceAll("\\s*\\]\\*\\*","").replaceAll("\\s", ""))
			.collect(Collectors.toSet());
		/*link2.getGws_linkReasons()
			.stream()
			.forEach(x -> x.replace("**[","").replace("]**","").trim())
			.forEach(linkReasonsWithoutMarkup::add);*/

		linkReasons.addAll(link1.getGws_linkReasons());
		//TODO use ontology and determine most specific link - use snippet of that
		// instead of using the first known snippet
		
		for (String linkReason : link2.getGws_linkReasons()) {
			String linkReasonWithoutMarkup = linkReason.replaceAll("\\*\\*\\[\\s*","").replaceAll("\\s*\\]\\*\\*","").replaceAll("\\s", "");
			if (!linkReasonsWithoutMarkup.contains(linkReasonWithoutMarkup)) linkReasons.add(linkReason);
			linkReasonsWithoutMarkup.add(linkReasonWithoutMarkup);
		}
		List<String> newLinkReasons = new ArrayList<>();
		newLinkReasons.addAll(linkReasons);
		link1.setGws_linkReasons(newLinkReasons);
		
		Set<String> linkReason_all = Arrays.stream(
				link1.getLinkReason()
				.split("@@@"))
					.filter(x -> (x != null && !x.isEmpty()))
					.collect(Collectors.toSet());
		Arrays.stream(link2.getLinkReason().split("@@@"))
			.filter(x -> (x != null && !x.isEmpty()))
			.forEach(linkReason_all::add);
		link1.setLinkReason(linkReason_all
				.stream()
				.collect(Collectors.joining("@@@")));
		
		Set<String> provenance_all = new HashSet<>();
		addIfExists(provenance_all, link1.getProvenance());
		addIfExists(provenance_all, link2.getProvenance());
		link1.setProvenance(provenance_all
				.stream()
				.collect(Collectors.joining(" / ")));
		// TODO what to do with entity relations and other fields?
		return link1;
	}
	
	protected void createElasticLinks(List<EntityLink> flattenedLinks, String index) throws ClientProtocolException, IOException {
		HttpClient httpclient = HttpClients.createDefault();

		for (EntityLink link : flattenedLinks) {
			Entity fromEntity = getInputDataStoreClient().get(Entity.class, link.getFromEntity().replaceAll(apiEntityPrefixRegex, apiEntityPrefixReplacement));
			Entity toEntity = getInputDataStoreClient().get(Entity.class, link.getToEntity().replaceAll(apiEntityPrefixRegex, apiEntityPrefixReplacement));
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
			else {
				if (!EntityType.citedData.equals(fromEntity.getEntityType()))
					fromEntity.setUri("literaturpool-" + SerializationUtils.getHexMd5(fromEntity.getEntityView()));
				else fromEntity.setUri(fromEntity.getUri().replaceAll("http://.*/entity/", "referencepool-"));
			}
			if (null != toEntity.getGwsId()) toEntity.setUri(toEntity.getGwsId());
			else {
				if (!EntityType.citedData.equals(toEntity.getEntityType()))
					toEntity.setUri("literaturpool-" + SerializationUtils.getHexMd5(toEntity.getEntityView()));
				else toEntity.setUri(toEntity.getUri().replaceAll("http://.*/entity/", "referencepool-"));
			}
			// TODO dedup entities:
			// check if similar entity is in temp data store (or other data store?)
			// if so, merge entities and merge links
			// for this, set up ingoingLinks and outgoingLinks maps
			//   containing entities as keys and in-/outgoing links as values
			// in those links, replace ID of entity with new merged ID

			/*link.setFromEntity(fromEntity.getUri());
			link.setToEntity(toEntity.getUri());*/

			//don't push link to index if its entities have too little metadata
			if (!showInGws(fromEntity, toEntity)) continue;
			ElasticLink elink = new ElasticLink(link);
			elink.setGws_fromView(fromEntity.getEntityView());
			elink.setGws_toView(toEntity.getEntityView());
			elink.setGws_fromType(fromEntity.getEntityType());
			elink.setGws_toType(toEntity.getEntityType());
			elink.setGws_fromID(fromEntity.getUri());
			elink.setGws_toID(toEntity.getUri());
			// set URI in a way that duplicates would be overriden
			// but: do not overwrite links; instead:
			//	search if links with such a uri already exist. 
			//      if so: merge linkReasons and confidence scores, pssibly other fields as well?
			List<String> _linkReasons = Arrays.stream(elink.getLinkReason().split("@@@"))
				.filter(x -> (x != null && !x.isEmpty()))
				.map(x -> x.replaceAll(apiTextualReferencePrefixRegex, apiTextualReferencePrefixReplacement))
				.collect(Collectors.toList());
			elink.setAndResolveGws_linkReasons(_linkReasons);
			// at this point, links are be disambiguated
			// i.e. all linkReasons must have the same reference
			String dataReference = "";
			if (null != elink.getLinkView() 
				&& ! elink.getLinkView().isEmpty()) 
					dataReference = elink.getLinkView().replaceAll("\\s+","").toLowerCase();
			//TODO THIS REPLACEMENT IS NOT CORRECT:
			//ALLBUS 2000-2002 would be the same as
			//ALLBUS 2000,2002...
			String linkUri = elink.getGws_fromID() + "---" + elink.getGws_toID() + "---" + dataReference.replaceAll("\\W", "_");

			ElasticLink duplicate = getLinkFromIndex(getExecution().getIndexDirectory() + "_search", linkUri);
			if (null != duplicate) elink = mergeLinks(duplicate, elink);

			elink.setUri(linkUri);

			if (toEntity.getEntityType().equals(EntityType.citedData)) elink.setGws_link(elink.getGwsLink(toEntity.getName().replaceAll("\\d", "").trim()));
			
			if (null != index) {
				HttpPut httpput = new HttpPut(index + "EntityLink/" + elink.getUri().replaceAll("http://.*/entityLink/", ""));
				put(httpclient, httpput, new StringEntity(SerializationUtils.toJSON(elink), ContentType.APPLICATION_JSON));
				log.debug(String.format("put link \"%s\" to %s", elink, index));
			}
			elinks.add(elink);
			entities.add(fromEntity);
			entities.add(toEntity);
		}
	}

	void pushToIndex(String index, List<EntityLink> links) throws ClientProtocolException, IOException {
		if (null == index || index.isEmpty()) {
			index = InfolisConfig.getElasticSearchIndex();
		}
		getExecution().setIndexDirectory(index);
		HttpClient httpclient = HttpClients.createDefault();
		
		createElasticLinks(links, index);
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
