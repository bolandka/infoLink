package io.github.infolis.infolink.querying;

import io.github.infolis.model.entity.Entity;
import io.github.infolis.infolink.querying.QueryService;
import io.github.infolis.algorithm.SearchResultLinker;

import java.util.Map;

/**
 * 
 * @author kata
 *
 */
public class ExpectedOutput {
	
	QueryService queryService;
	Entity entity;
	Map<String, String> doiTitleMap;
	Class<? extends SearchResultLinker> searchResulterLinkerClass;
		
	ExpectedOutput(QueryService queryService, Entity entity, Class<? extends SearchResultLinker> searchResultLinkerClass, Map<String, String> doiTitleMap) {
		this.queryService = queryService;
		this.entity = entity;
		this.searchResulterLinkerClass = searchResultLinkerClass;
		this.doiTitleMap = doiTitleMap;
	}
		
	QueryService getQueryService() {
		return this.queryService;
	}
		
	Entity getEntity() {
		return this.entity;
	}
		
	Class<? extends SearchResultLinker> getSearchResultLinkerClass() {
		return this.searchResulterLinkerClass;
	}
		
	Map<String, String> getDoiTitleMap() {
		return this.doiTitleMap;
	}
}
	
