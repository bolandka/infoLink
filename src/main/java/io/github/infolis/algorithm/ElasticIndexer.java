package io.github.infolis.algorithm;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
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
import com.google.common.collect.Multimap;

import io.github.infolis.InfolisConfig;
import io.github.infolis.datastore.DataStoreClient;
import io.github.infolis.datastore.FileResolver;
import io.github.infolis.model.entity.Entity;
import io.github.infolis.model.EntityType;
import io.github.infolis.model.entity.EntityLink;
import io.github.infolis.model.TextualReference;
import io.github.infolis.util.SerializationUtils;

public abstract class ElasticIndexer extends BaseAlgorithm {

	private static final Logger log = LoggerFactory.getLogger(ElasticIndexer.class);
	
	public ElasticIndexer(DataStoreClient inputDataStoreClient, DataStoreClient outputDataStoreClient,
			FileResolver inputFileResolver, FileResolver outputFileResolver) {
		super(inputDataStoreClient, outputDataStoreClient, inputFileResolver, outputFileResolver);
	}
	
	protected void put(HttpClient httpclient, HttpPut httpput, StringEntity data) throws ClientProtocolException, IOException {
		httpput.setEntity(data);
		httpput.setHeader("content-type", ContentType.APPLICATION_JSON.toString());
		httpput.setHeader("Accept", ContentType.APPLICATION_JSON.toString());

		HttpResponse response = httpclient.execute(httpput);
		HttpEntity entity = response.getEntity();
			
		if (entity != null) {
		    InputStream instream = entity.getContent();
		    try {
		        log.debug(IOUtils.toString(instream));
		    } finally {
		        instream.close();
		    }
		}
	}
	
	protected void post(HttpClient httpclient, HttpPost httppost, StringEntity data) throws ClientProtocolException, IOException {
		httppost.setEntity(data);
		httppost.setHeader("content-type", ContentType.APPLICATION_JSON.toString());
		httppost.setHeader("Accept", ContentType.APPLICATION_JSON.toString());

		HttpResponse response = httpclient.execute(httppost);
		HttpEntity entity = response.getEntity();
			
		if (entity != null) {
		    InputStream instream = entity.getContent();
		    try {
		        log.debug(IOUtils.toString(instream));
		    } finally {
		        instream.close();
		    }
		}
	}

	public class ElasticLink extends EntityLink {
		private EntityType gws_fromType;
		private EntityType gws_toType;
		private String gws_fromView;
		private String gws_toView;
		private String gws_fromID;
		private String gws_toID;
		private String gws_link;

		private Map<String, String> getDataUrls(String filename) {
			Map<String, String> data_urls = new HashMap<>();
			try {
				String content = FileUtils.readFileToString(new File(filename));
				for (String line : content.trim().split("\n")) {
					String[] keyValue = line.split(";");
					data_urls.put(keyValue[0].trim(), keyValue[1].trim());
				}
			} catch (IOException e) {
				log.error(e.toString());
			}
			return data_urls;
		}

		private String getUrlForCitedData(String citedData, Map<String, String> data_urls) 		{
			String dataLink = data_urls.get(citedData);
			if (null != dataLink) {
				log.debug("FOUND DATA LINK FOR " + citedData + ": " + dataLink);
				return dataLink;
			} else {
				log.debug("NO DATA LINK FOR " + citedData);
				return null;
			}
		}

		public String getGwsLink(String citedData, String urlFilename) {
			Map<String, String> data_urls = getDataUrls(urlFilename);
			return getUrlForCitedData(citedData, data_urls);
		}

		protected String getGwsLink(String citedData) {
			Map<String, String> data_urls = getDataUrls("/infolis-files/data-urls.csv");
			return getUrlForCitedData(citedData, data_urls);
		}

		public ElasticLink() {}

		public ElasticLink(EntityLink copyFrom) {
			this.setFromEntity(copyFrom.getFromEntity());
			this.setToEntity(copyFrom.getToEntity());
			this.setGws_fromID(copyFrom.getFromEntity());
			this.setGws_toID(copyFrom.getToEntity());
			this.setConfidence(copyFrom.getConfidence());
			String linkReason = copyFrom.getLinkReason();
			if (null != linkReason && !linkReason.isEmpty()) this.setLinkReason(removePtb3escaping(resolveLinkReason(linkReason)));
			this.setEntityRelations(copyFrom.getEntityRelations());
			this.setProvenance(copyFrom.getProvenance());
			this.setLinkView(copyFrom.getLinkView());
			this.setGws_link(null);
		}

		private String removePtb3escaping(String string) {
			return string.replaceAll("-LRB-", "(").replaceAll("-RRB-", ")");
		}

		private String resolveLinkReason(String uri) {
			return getInputDataStoreClient().get(TextualReference.class, uri).toPrettyString();
		}

		public void setGws_fromID(String gws_fromID) {
			this.gws_fromID = gws_fromID;
		}

		public String getGws_fromID() {
			return this.gws_fromID;
		}

		public String getGws_toID() {
			return this.gws_toID;
		}

		public void setGws_toID(String gws_toID) {
			this.gws_toID = gws_toID;
		}

		public void setGws_fromType(EntityType gws_fromType) {
			this.gws_fromType = gws_fromType;
		}

		public EntityType getGws_fromType() {
			return this.gws_fromType;
		}

		public void setGws_toType(EntityType gws_toType) {
			this.gws_toType = gws_toType;
		}

		public EntityType getGws_toType() {
			return this.gws_toType;
		}

		public void setGws_fromView(String gws_fromView) {
			this.gws_fromView = gws_fromView;
		}

		public String getGws_fromView() {
			return this.gws_fromView;
		}
		
		public void setGws_toView(String gws_toView) {
			this.gws_toView = gws_toView;
		}

		public String getGws_toView() {
			return this.gws_toView;
		}

		public void setGws_link(String link) {
			this.gws_link = link;
		}

		public String getGws_link() {
			return this.gws_link;
		}
	
	}	

	protected boolean showInGws(Entity fromEntity, Entity toEntity) {
		if (basicPublicationMetadataExists(fromEntity) && basicPublicationMetadataExists(toEntity)) return true;
		return false;
	}

	private boolean basicPublicationMetadataExists(Entity entity) {
		// apply filter on publications only
		if (entity.getEntityType() != EntityType.publication) return true;
		// ignore all entities where not even the basic metadata (title, author, year) is known
		if ((null == entity.getGwsId() || entity.getGwsId().isEmpty()) && (null == entity.getName() || entity.getName().isEmpty() || null == entity.getAuthors() || entity.getAuthors().isEmpty() || null == entity.getYear() || entity.getYear().isEmpty())) return false;
		return true;
	}

	
	abstract void pushToIndex(String index, List<EntityLink> links) throws ClientProtocolException, IOException;

	abstract List<EntityLink> getLinksToPush();

	private void getData(List<String> ignoreLinksWithProvenance) {
		Multimap<String, String> query = ArrayListMultimap.create();
		for (EntityLink link : getInputDataStoreClient().search(EntityLink.class, query)) {
			if (null != ignoreLinksWithProvenance && !ignoreLinksWithProvenance.isEmpty()) {
				for (String provenance : ignoreLinksWithProvenance) {
					if (link.getProvenance().equals(provenance)) continue;
				}
			}
			getExecution().getLinks().add(link.getUri());
		}
	}
	

	@Override
	public void execute() throws IOException {
		getExecution().setLinks(new ArrayList<>());
		getExecution().setLinkedEntities(new ArrayList<>());
		// standard: ignore "infolis-ontology" data...
		getData(getExecution().getSeeds());		
		pushToIndex(getExecution().getIndexDirectory(), getLinksToPush());
		
	}

	@Override
	public void validate() throws IllegalAlgorithmArgumentException {
		// TODO Auto-generated method stub
		
	}
	
}
