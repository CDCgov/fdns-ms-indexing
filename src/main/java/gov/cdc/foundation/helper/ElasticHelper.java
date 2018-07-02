package gov.cdc.foundation.helper;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import gov.cdc.helper.ObjectHelper;
import gov.cdc.helper.common.ServiceException;

@Component
public class ElasticHelper {

	private static final Logger logger = Logger.getLogger(ElasticHelper.class);

	private static ElasticHelper instance;
	private static RestClient client;

	private String host;
	private String protocol;
	private int port;

	public ElasticHelper(@Value("${elastic.host}") String host, @Value("${elastic.port}") int port, @Value("${elastic.protocol}") String protocol) {
		this.host = host;
		this.port = port;
		this.protocol = protocol;
		instance = this;
	}

	public static ElasticHelper getInstance() {
		return instance;
	}

	public RestClient getClient() throws UnknownHostException {
		if (client == null)
			client = RestClient.builder(new HttpHost(host, port, protocol)).build();
		return client;
	}

	public Response index(JSONObject data, String index, String type, String id) throws ServiceException {
		try {
			RestClient client = getClient();
			try (NStringEntity entity = new NStringEntity(data.toString(), ContentType.APPLICATION_JSON)) {
				return client.performRequest("PUT", String.format("/%s/%s/%s", index, type, id), Collections.<String, String>emptyMap(), entity);
			}
		} catch (Exception e) {
			handleException(e);
			return null;
		}
	}

	public Response getObject(String index, String type, String id) throws ServiceException {
		try {
			RestClient client = getClient();
			return client.performRequest("GET", String.format("/%s/%s/%s", index, type, id), Collections.<String, String>emptyMap());
		} catch (Exception e) {
			handleException(e);
			return null;
		}
	}

	public Response searchObjects(String index, JSONObject query, int from, int size, String scroll, JSONObject append) throws ServiceException {
		try {
			RestClient client = getClient();
			JSONObject elkQuery = new JSONObject();
			elkQuery.put("from", from);
			elkQuery.put("size", size);
			if (query != null) {
				elkQuery.put("query", query);
			}
			if (append != null) {
				for (Object key : append.keySet()) {
					elkQuery.put((String) key, append.get((String) key));
				}
			}
			
			String url = String.format("/%s/_search", index);
			if (scroll != null && !scroll.isEmpty())
				url += "?scroll=" + scroll;

			try (NStringEntity entity = new NStringEntity(elkQuery.toString(), ContentType.APPLICATION_JSON)) {
				return client.performRequest("GET", url, Collections.<String, String>emptyMap(), entity);
			}
		} catch (Exception e) {
			handleException(e);
			return null;
		}
	}

	public Response scrollSearch(String scrollId, String scrollLiveTime) throws ServiceException {
		try {
			RestClient client = getClient();
			
			JSONObject query = new JSONObject();
			query.put("scroll", scrollLiveTime);
			query.put("scroll_id", scrollId);
			
			try (NStringEntity entity = new NStringEntity(query.toString(), ContentType.APPLICATION_JSON)) {
				return client.performRequest("POST", "/_search/scroll", Collections.<String, String>emptyMap(), entity);
			}
		} catch (Exception e) {
			handleException(e);
			return null;
		}
	}

	public Response deleteScrollIndex(String scrollId) throws ServiceException {
		try {
			RestClient client = getClient();
			
			JSONObject query = new JSONObject();
			query.put("scroll_id", new JSONArray());
			query.getJSONArray("scroll_id").put(scrollId);
			
			try (NStringEntity entity = new NStringEntity(query.toString(), ContentType.APPLICATION_JSON)) {
				return client.performRequest("DELETE", "/_search/scroll", Collections.<String, String>emptyMap(), entity);
			}
		} catch (Exception e) {
			handleException(e);
			return null;
		}
	}
	
	public Response defineMapping(String index, String type, JSONObject payload) throws ServiceException {
		try {
			RestClient client = getClient();
			try (NStringEntity entity = new NStringEntity(payload.toString(), ContentType.APPLICATION_JSON)) {
				return client.performRequest("PUT", String.format("/%s/_mapping/%s", index, type), Collections.<String, String>emptyMap(), entity);
			}
		} catch (Exception e) {
			handleException(e);
			return null;
		}
	}

	public Response createIndex(String index) throws ServiceException {
		try {
			RestClient client = getClient();
			return client.performRequest("PUT", String.format("/%s", index), Collections.<String, String>emptyMap());
		} catch (Exception e) {
			handleException(e);
			return null;
		}
	}

	public Response deleteIndex(String index) throws ServiceException {
		try {
			RestClient client = getClient();
			return client.performRequest("DELETE", String.format("/%s", index), Collections.<String, String>emptyMap());
		} catch (Exception e) {
			handleException(e);
			return null;
		}
	}

	public void hydrate(String authorizationHeader, JSONObject elkObject, String database, String collection, String objectId) throws ServiceException {
		JSONObject merged;
		try {
			ObjectHelper helper = ObjectHelper.getInstance(authorizationHeader);
			JSONObject object = helper.getObject(objectId, database, collection);
			merged = helper.merge(object, elkObject.getJSONObject("_source"));
			elkObject.put("_source", merged);
		} catch (Exception e) {
			throw new ServiceException(e);
		}
	}

	public void hydrate(String authorizationHeader, JSONArray hits, String database, String collection) throws ServiceException {
		for (int i = 0; i < hits.length(); i++) {
			JSONObject hit = hits.getJSONObject(i);
			hydrate(authorizationHeader, hit, database, collection, hit.getString("_id"));
		}
	}

	private void handleException(Exception e) throws ServiceException {
		if (e instanceof ResponseException) {
			logger.error(e);
			try {
				ResponseException re = (ResponseException) e;
				String responseStr = IOUtils.toString(re.getResponse().getEntity().getContent());
				throw new ServiceException(new JSONObject(responseStr));
			} catch (IOException e2) {
				throw new ServiceException(e2);
			}
		} else
			throw new ServiceException(e);

	}

}
