package io.github.infolis.algorithm;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.infolis.InfolisConfig;
import io.github.infolis.datastore.DataStoreClient;
import io.github.infolis.datastore.FileResolver;
import io.github.infolis.model.entity.Entity;
import io.github.infolis.model.EntityType;
import io.github.infolis.model.entity.EntityLink;
import io.github.infolis.util.SerializationUtils;

public class DbIndexer extends ElasticIndexer {

	private static final Logger log = LoggerFactory.getLogger(DbIndexer.class);
	
	public DbIndexer(DataStoreClient inputDataStoreClient, DataStoreClient outputDataStoreClient,
			FileResolver inputFileResolver, FileResolver outputFileResolver) {
		super(inputDataStoreClient, outputDataStoreClient, inputFileResolver, outputFileResolver);
	}
	
	List<EntityLink> getLinksToPush() {
		return getInputDataStoreClient().get(EntityLink.class, getExecution().getLinks());
	}	
	
	
	void pushToIndex(String index, List<EntityLink> links) throws ClientProtocolException, IOException {
		if (null == index || index.isEmpty()) index = InfolisConfig.getElasticSearchIndex();
		HttpClient httpclient = HttpClients.createDefault();
		List<Entity> entities = new ArrayList<>();
		
		for (EntityLink link : links) {
			Entity fromEntity = getInputDataStoreClient().get(Entity.class, link.getFromEntity().replaceAll("http.*/entity", "http://svkolodtest.gesis.intra/link-db/api/entity"));
			Entity toEntity = getInputDataStoreClient().get(Entity.class, link.getToEntity().replaceAll("http.*/entity", "http://svkolodtest.gesis.intra/link-db/api/entity"));
			// do not post entities or their links having no gwsId
			//if ((null == fromEntity.getGwsId()) || (null == toEntity.getGwsId())) continue;
			// TODO check if entityType == entityType.citedData and don't add prefix "literaturpool-" in this case if that prefix should be applied to publications only
			if (null != fromEntity.getGwsId()) fromEntity.setUri(fromEntity.getGwsId());
			else fromEntity.setUri(fromEntity.getUri().replaceAll("http.*/entity/", "literaturpool-"));
			if (null != toEntity.getGwsId()) toEntity.setUri(toEntity.getGwsId());
			else toEntity.setUri(toEntity.getUri().replaceAll("http.*/entity/", "literaturpool-"));
		
			// always post entities; links only when to be shown in gws	
			if (showInGws(fromEntity, toEntity)) {
				link.setFromEntity(fromEntity.getUri());
				link.setToEntity(toEntity.getUri());

				ElasticLink elink = new ElasticLink(link);
				elink.setGws_fromType(fromEntity.getEntityType());
				elink.setGws_toType(toEntity.getEntityType());
				if (EntityType.citedData.equals(toEntity.getEntityType())) {
					log.debug("searching for gwsLink for " + toEntity.getName());
					elink.setGws_link(elink.getGwsLink(toEntity.getName().replaceAll("\\d", "").trim()));
					//elink.setGws_link(elink.getGwsLink(toEntity.getName().trim()));
					//log.debug(elink.getGws_link());
				} else if (EntityType.citedData.equals(fromEntity.getEntityType())) {
					log.debug("Searching for gwsLink for " + fromEntity.getName());
					elink.setGws_link(elink.getGwsLink(fromEntity.getName().replaceAll("\\d", "").trim()));
					//elink.setGws_link(elink.getGwsLink(fromEntity.getName().trim()));
				}
				elink.setGws_fromView(fromEntity.getEntityView());
				elink.setGws_toView(toEntity.getEntityView());
				elink.setUri(fromEntity.getUri() + "---" + toEntity.getUri());
				HttpPut httpput = new HttpPut(index + "EntityLink/" + elink.getUri());
				log.debug(SerializationUtils.toJSON(elink).toString());
				put(httpclient, httpput, new StringEntity(SerializationUtils.toJSON(elink), ContentType.APPLICATION_JSON));
				//post(httpclient, httpost, new StringEntity(elink.toJson(), ContentType.APPLICATION_JSON));
				//log.debug(String.format("posted link \"%s\" to %s", link, index));
				//if (elink.getGws_link() != null) throw new RuntimeException();
			}

			entities.add(fromEntity);
			entities.add(toEntity);
		}

		//for (String entity : getExecution().getLinkedEntities()) {
		//	Entity e = getInputDataStoreClient().get(Entity.class, entity);
		for (Entity e : entities) {
			if (null == e.getGwsId() || e.getGwsId().isEmpty()) e.setGwsId(e.getUri());
			//log.debug("putting: " + SerializationUtils.toJSON(e));
			HttpPut httpput = new HttpPut(index + "Entity/" + e.getUri());
			put(httpclient, httpput, new StringEntity(SerializationUtils.toJSON(e), ContentType.APPLICATION_JSON));
			if (null == e.getUri()) log.warn(String.format("uri is null: cannot put entity \"%s\" to %s", SerializationUtils.toJSON(e), index));
		}
		
	}
	
}
