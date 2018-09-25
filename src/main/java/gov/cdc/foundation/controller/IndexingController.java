package gov.cdc.foundation.controller;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.elasticsearch.client.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.mongodb.BasicDBObject;

import gov.cdc.foundation.helper.ConfigurationHelper;
import gov.cdc.foundation.helper.ElasticHelper;
import gov.cdc.foundation.helper.JSONHelper;
import gov.cdc.foundation.helper.LoggerHelper;
import gov.cdc.foundation.helper.MessageHelper;
import gov.cdc.foundation.helper.QueryBuilder;
import gov.cdc.helper.ErrorHandler;
import gov.cdc.helper.ObjectHelper;
import gov.cdc.helper.common.ServiceException;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import springfox.documentation.annotations.ApiIgnore;

@Controller
@EnableAutoConfiguration
@RequestMapping("/api/1.0/")
public class IndexingController {

	private static final Logger logger = Logger.getLogger(IndexingController.class);

	@Value("${version}")
	private String version;

	private static final String CONST_MONGO_DATABASE = "$.mongo.database";
	private static final String CONST_MONGO_COLLECTION = "$.mongo.collection";
	private static final String CONST_ELASTIC_INDEX = "$.elastic.index";
	private static final String CONST_ELASTIC_TYPE = "$.elastic.type";

	private String configRegex;

	public IndexingController(@Value("${config.regex}") String configRegex) {
		this.configRegex = configRegex;
	}

	@RequestMapping(method = RequestMethod.GET)
	@ResponseBody
	public ResponseEntity<?> index() throws IOException {
		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_INDEX, null);

		try {
			JSONObject json = new JSONObject();
			json.put("version", version);
			return new ResponseEntity<>(mapper.readTree(json.toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_INDEX, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}


	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName))")
	@RequestMapping(value = "index/{config}/{id}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Index an existing stored object.", notes = "Index an existing stored object.")
	@ResponseBody
	public ResponseEntity<?> indexObject(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@ApiParam(value = "Config Name") @PathVariable(value = "config") String configName,
			@ApiParam(value = "Object Id") @PathVariable(value = "id") String objectId) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_INDEXOBJECT, null);
		log.put(MessageHelper.CONST_METHOD, MessageHelper.METHOD_INDEXOBJECT);
		log.put(MessageHelper.CONST_OBJECTTYPE, configName);
		log.put(MessageHelper.CONST_OBJECTID, objectId);

		try {
			JSONObject config = ConfigurationHelper.getInstance().getConfiguration(configName, authorizationHeader);

			Object document = Configuration.defaultConfiguration().jsonProvider().parse(config.toString());
			String database = JsonPath.read(document, IndexingController.CONST_MONGO_DATABASE);
			String collection = JsonPath.read(document, IndexingController.CONST_MONGO_COLLECTION);
			String index = JsonPath.read(document, IndexingController.CONST_ELASTIC_INDEX);
			String type = JsonPath.read(document, IndexingController.CONST_ELASTIC_TYPE);

			if (StringUtils.isEmpty(database))
				throw new ServiceException(MessageHelper.ERROR_NO_DATABASE);
			if (StringUtils.isEmpty(collection))
				throw new ServiceException(MessageHelper.ERROR_NO_COLLECTION);
			if (StringUtils.isEmpty(index))
				throw new ServiceException(MessageHelper.ERROR_NO_INDEX);
			if (StringUtils.isEmpty(type))
				throw new ServiceException(MessageHelper.ERROR_NO_TYPE);

			ObjectHelper helper = ObjectHelper.getInstance(authorizationHeader);
			if (!helper.exists(objectId, database, collection))
				throw new ServiceException(MessageHelper.ERROR_NO_OBJECT);
			JSONObject object = helper.getObject(objectId, database, collection);

			prepareObject(object, config);

			JSONObject response = new JSONObject();
			response.put("data", object);

			// Index the object
			Response elkResponse = ElasticHelper.getInstance().index(object, index, type, objectId);
			String elkResponseStr = IOUtils.toString(elkResponse.getEntity().getContent(), Charsets.UTF_8);
			response.put("elk", new JSONObject(elkResponseStr));

			return new ResponseEntity<>(mapper.readTree(response.toString()), HttpStatus.OK);

		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_INDEXOBJECT, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName))")
	@RequestMapping(value = "index/bulk/{config}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Index a list of objects.", notes = "Index a list of objects.")
	@ResponseBody
	public ResponseEntity<?> indexBulkObjects(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@ApiParam(value = "Config Name") @PathVariable(value = "config") String configName,
			@ApiParam(value = "JSON array of object ids") @RequestBody String data) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_INDEXBULKOBJECTS, null);
		log.put(MessageHelper.CONST_METHOD, MessageHelper.METHOD_INDEXBULKOBJECTS);
		log.put(MessageHelper.CONST_OBJECTTYPE, configName);

		try {
			JSONArray arrayOfIds = new JSONArray(data);
			if (arrayOfIds.length() > 100)
				throw new ServiceException(MessageHelper.ERROR_BULK_MAX);

			JSONObject config = ConfigurationHelper.getInstance().getConfiguration(configName, authorizationHeader);

			Object document = Configuration.defaultConfiguration().jsonProvider().parse(config.toString());
			String database = JsonPath.read(document, IndexingController.CONST_MONGO_DATABASE);
			String collection = JsonPath.read(document, IndexingController.CONST_MONGO_COLLECTION);
			String index = JsonPath.read(document, IndexingController.CONST_ELASTIC_INDEX);
			String type = JsonPath.read(document, IndexingController.CONST_ELASTIC_TYPE);

			if (StringUtils.isEmpty(database))
				throw new ServiceException(MessageHelper.ERROR_NO_DATABASE);
			if (StringUtils.isEmpty(collection))
				throw new ServiceException(MessageHelper.ERROR_NO_COLLECTION);
			if (StringUtils.isEmpty(index))
				throw new ServiceException(MessageHelper.ERROR_NO_INDEX);
			if (StringUtils.isEmpty(type))
				throw new ServiceException(MessageHelper.ERROR_NO_TYPE);

			ObjectId[] objArray = new ObjectId[arrayOfIds.length()];
			for (int i = 0; i < arrayOfIds.length(); i++) {
				objArray[i] = new ObjectId(arrayOfIds.getString(i));
			}
			BasicDBObject inQuery = new BasicDBObject("$in", objArray);
			BasicDBObject query = new BasicDBObject("_id", inQuery);

			JSONObject object = ObjectHelper.getInstance(authorizationHeader).find(new JSONObject(query.toString()), database, collection);
			JSONArray items = object.getJSONArray("items");
			for (int i = 0; i < items.length(); i++) {
				prepareObject(items.getJSONObject(i), config);
				ElasticHelper.getInstance().index(object, index, type, arrayOfIds.getString(i));
			}

			JSONObject response = new JSONObject();
			response.put("indexed", items.length());
			response.put("success", true);

			return new ResponseEntity<>(mapper.readTree(response.toString()), HttpStatus.OK);

		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_INDEXOBJECT, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName))")
	@RequestMapping(value = "index/all/{config}", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Index all objects in MongoDB.", notes = "Index all objects in MongoDB.")
	@ResponseBody
	public ResponseEntity<?> indexAll(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@ApiParam(value = "Config Name") @PathVariable(value = "config") String configName) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_INDEXALL, null);
		log.put(MessageHelper.CONST_METHOD, MessageHelper.METHOD_INDEXALL);
		log.put(MessageHelper.CONST_OBJECTTYPE, configName);

		logger.info("Test");

		try {
			JSONObject config = ConfigurationHelper.getInstance().getConfiguration(configName, authorizationHeader);

			Object document = Configuration.defaultConfiguration().jsonProvider().parse(config.toString());
			String database = JsonPath.read(document, IndexingController.CONST_MONGO_DATABASE);
			String collection = JsonPath.read(document, IndexingController.CONST_MONGO_COLLECTION);
			String index = JsonPath.read(document, IndexingController.CONST_ELASTIC_INDEX);
			String type = JsonPath.read(document, IndexingController.CONST_ELASTIC_TYPE);

			if (StringUtils.isEmpty(database))
				throw new ServiceException(MessageHelper.ERROR_NO_DATABASE);
			if (StringUtils.isEmpty(collection))
				throw new ServiceException(MessageHelper.ERROR_NO_COLLECTION);
			if (StringUtils.isEmpty(index))
				throw new ServiceException(MessageHelper.ERROR_NO_INDEX);
			if (StringUtils.isEmpty(type))
				throw new ServiceException(MessageHelper.ERROR_NO_TYPE);

			// Start the index asynchronously
			new Thread(new Runnable() {
				public void run() {
					try {
						indexAll(authorizationHeader, database, collection, index, type, config);
					} catch (Exception e) {
						logger.error(e);
					}
				}
			}).start();

			JSONObject response = new JSONObject();
			response.put("success", true);

			return new ResponseEntity<>(mapper.readTree(response.toString()), HttpStatus.OK);

		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_INDEXOBJECT, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@Async
	private void indexAll(String authorizationHeader, String database, String collection, String index, String type, JSONObject config) throws ServiceException, IOException {
		ObjectHelper helper = ObjectHelper.getInstance(authorizationHeader);

		// Count all items in MongoDB
		int nbOfItems = helper.countObjects(new JSONObject(), database, collection).getInt("count");
		logger.debug("# of items: " + nbOfItems);

		int currentIndex = 0;
		while (currentIndex < nbOfItems) {
			logger.debug(String.format("  Indexing [ %5d ~ %5d ] / %5d...", currentIndex, currentIndex + 99, nbOfItems));
			// Query
			JSONObject object = helper.find(new JSONObject(), database, collection, currentIndex, 100);
			JSONArray items = object.getJSONArray("items");
			for (int i = 0; i < items.length(); i++) {
				JSONObject item = items.getJSONObject(i);
				String id = item.getJSONObject("_id").getString("$oid");
				prepareObject(item, config);
				try {
					ElasticHelper.getInstance().index(item, index, type, id);
				} catch (Exception e) {
					logger.error("Error with object: " + id);
					logger.error(e);
				}
			}
			currentIndex += 100;
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName))")
	@RequestMapping(value = "get/{config}/{id}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Get an indexed object.", notes = "Get an indexed object.")
	@ResponseBody
	public ResponseEntity<?> getObject(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@ApiParam(value = "Config Name") @PathVariable(value = "config") String configName,
			@ApiParam(value = "Object Id") @PathVariable(value = "id") String objectId,
			@ApiParam(value = "Hydrate") @RequestParam(value = "hydrate", required = false, defaultValue = "false") boolean hydrate) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_GETOBJECT, null);
		log.put(MessageHelper.CONST_METHOD, MessageHelper.METHOD_GETOBJECT);
		log.put(MessageHelper.CONST_OBJECTTYPE, configName);
		log.put(MessageHelper.CONST_OBJECTID, objectId);

		try {
			JSONObject config = ConfigurationHelper.getInstance().getConfiguration(configName, authorizationHeader);

			Object document = Configuration.defaultConfiguration().jsonProvider().parse(config.toString());
			String index = JsonPath.read(document, IndexingController.CONST_ELASTIC_INDEX);
			String type = JsonPath.read(document, IndexingController.CONST_ELASTIC_TYPE);

			if (StringUtils.isEmpty(index))
				throw new ServiceException(MessageHelper.ERROR_NO_INDEX);
			if (StringUtils.isEmpty(type))
				throw new ServiceException(MessageHelper.ERROR_NO_TYPE);

			Response elkResponse = ElasticHelper.getInstance().getObject(index, type, objectId);
			String elkResponseStr = IOUtils.toString(elkResponse.getEntity().getContent(), Charsets.UTF_8);

			if (hydrate) {
				String database = JsonPath.read(document, IndexingController.CONST_MONGO_DATABASE);
				String collection = JsonPath.read(document, IndexingController.CONST_MONGO_COLLECTION);

				if (StringUtils.isEmpty(database))
					throw new ServiceException(MessageHelper.ERROR_NO_DATABASE);
				if (StringUtils.isEmpty(collection))
					throw new ServiceException(MessageHelper.ERROR_NO_COLLECTION);

				JSONObject elkObject = new JSONObject(elkResponseStr);
				ElasticHelper.getInstance().hydrate(authorizationHeader, elkObject, database, collection, objectId);

				return new ResponseEntity<>(mapper.readTree(elkObject.toString()), HttpStatus.OK);
			} else
				return new ResponseEntity<>(mapper.readTree(elkResponseStr), HttpStatus.OK);

		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_GETOBJECT, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName))")
	@RequestMapping(value = "search/{config}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Search object.", notes = "Search indexed object.")
	@ResponseBody
	public ResponseEntity<?> searchObjects(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@ApiParam(value = "Config Name") @PathVariable(value = "config") String configName,
			@ApiParam(value = "Search query") @RequestParam(value = "query", required = false) String query,
			@ApiParam(value = "Hydrate") @RequestParam(value = "hydrate", required = false, defaultValue = "false") boolean hydrate,
			@ApiParam(value = "From") @RequestParam(value = "from", required = false, defaultValue = "0") int from,
			@ApiParam(value = "Size") @RequestParam(value = "size", required = false, defaultValue = "100") int size,
			@ApiParam(value = "Scroll live time (like 1m)") @RequestParam(value = "scroll", required = false, defaultValue = "") String scroll) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_SEARCHOBJECT, null);
		log.put(MessageHelper.CONST_METHOD, MessageHelper.METHOD_SEARCHOBJECT);
		log.put(MessageHelper.CONST_OBJECTTYPE, configName);

		try {
			JSONObject config = ConfigurationHelper.getInstance().getConfiguration(configName, authorizationHeader);

			Object document = Configuration.defaultConfiguration().jsonProvider().parse(config.toString());
			String index = JsonPath.read(document, IndexingController.CONST_ELASTIC_INDEX);

			if (StringUtils.isEmpty(index))
				throw new ServiceException(MessageHelper.ERROR_NO_INDEX);

			// Build query
			JSONObject queryObj = QueryBuilder.getInstance().parse(config, query);

			// Check if we need to append items to the query
			JSONObject append = null;
			if (config.has("appendToQuery"))
				append = config.getJSONObject("appendToQuery");

			Response elkResponse = ElasticHelper.getInstance().searchObjects(index, queryObj, from, size, scroll, append);
			String elkResponseStr = IOUtils.toString(elkResponse.getEntity().getContent(), Charsets.UTF_8);
			JSONObject elkObject = new JSONObject(elkResponseStr);
			elkObject.put("query", queryObj);

			if (hydrate) {
				String database = JsonPath.read(document, IndexingController.CONST_MONGO_DATABASE);
				String collection = JsonPath.read(document, IndexingController.CONST_MONGO_COLLECTION);

				if (StringUtils.isEmpty(database))
					throw new ServiceException(MessageHelper.ERROR_NO_DATABASE);
				if (StringUtils.isEmpty(collection))
					throw new ServiceException(MessageHelper.ERROR_NO_COLLECTION);

				ElasticHelper.getInstance().hydrate(authorizationHeader, elkObject.getJSONObject("hits").getJSONArray("hits"), database, collection);
			}

			return new ResponseEntity<>(mapper.readTree(elkObject.toString()), HttpStatus.OK);

		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_SEARCHOBJECT, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName))")
	@RequestMapping(value = "search/scroll/{config}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Scroll search result.", notes = "Scroll search result.")
	@ResponseBody
	public ResponseEntity<?> scrollSearch(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@ApiParam(value = "Config Name") @PathVariable(value = "config") String configName,
			@ApiParam(value = "Scroll live time (like 1m)") @RequestParam(value = "scroll", required = true, defaultValue = "1m") String scroll,
			@ApiParam(value = "Scroll identifier") @RequestParam(value = "scrollId", required = true, defaultValue = "") String scrollId,
			@ApiParam(value = "Hydrate") @RequestParam(value = "hydrate", required = false, defaultValue = "false") boolean hydrate) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_SCROLL, null);
		log.put(MessageHelper.CONST_METHOD, MessageHelper.METHOD_SCROLL);

		try {
			JSONObject config = ConfigurationHelper.getInstance().getConfiguration(configName, authorizationHeader);
			Object document = Configuration.defaultConfiguration().jsonProvider().parse(config.toString());

			Response elkResponse = ElasticHelper.getInstance().scrollSearch(scrollId, scroll);
			String elkResponseStr = IOUtils.toString(elkResponse.getEntity().getContent(), Charsets.UTF_8);
			JSONObject elkObject = new JSONObject(elkResponseStr);

			if (hydrate) {
				String database = JsonPath.read(document, IndexingController.CONST_MONGO_DATABASE);
				String collection = JsonPath.read(document, IndexingController.CONST_MONGO_COLLECTION);

				if (StringUtils.isEmpty(database))
					throw new ServiceException(MessageHelper.ERROR_NO_DATABASE);
				if (StringUtils.isEmpty(collection))
					throw new ServiceException(MessageHelper.ERROR_NO_COLLECTION);

				ElasticHelper.getInstance().hydrate(authorizationHeader, elkObject.getJSONObject("hits").getJSONArray("hits"), database, collection);
			}

			return new ResponseEntity<>(mapper.readTree(elkObject.toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_SEARCHOBJECT, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@RequestMapping(value = "search/scroll", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Delete Scroll Index.", notes = "Delete Scroll Index.")
	@ResponseBody
	public ResponseEntity<?> deleteScrollIndex(@ApiParam(value = "Scroll identifier") @RequestParam(value = "scrollId", required = true, defaultValue = "") String scrollId) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_SCROLL, null);
		log.put(MessageHelper.CONST_METHOD, MessageHelper.METHOD_SCROLL);

		try {
			Response elkResponse = ElasticHelper.getInstance().deleteScrollIndex(scrollId);
			String elkResponseStr = IOUtils.toString(elkResponse.getEntity().getContent(), Charsets.UTF_8);
			JSONObject elkObject = new JSONObject(elkResponseStr);

			return new ResponseEntity<>(mapper.readTree(elkObject.toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_SEARCHOBJECT, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName))")
	@RequestMapping(value = "mapping/{config}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Define a mapping in elasticsearch.", notes = "Define a mapping in elasticsearch.")
	@ResponseBody
	public ResponseEntity<?> defineMapping(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@RequestBody String payload,
			@ApiParam(value = "Config Name") @PathVariable(value = "config") String configName) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_DEFINEMAPPING, null);
		log.put(MessageHelper.CONST_METHOD, MessageHelper.METHOD_DEFINEMAPPING);
		log.put(MessageHelper.CONST_OBJECTTYPE, configName);

		try {
			JSONObject config = ConfigurationHelper.getInstance().getConfiguration(configName, authorizationHeader);

			Object document = Configuration.defaultConfiguration().jsonProvider().parse(config.toString());
			String index = JsonPath.read(document, IndexingController.CONST_ELASTIC_INDEX);
			String type = JsonPath.read(document, IndexingController.CONST_ELASTIC_TYPE);

			if (StringUtils.isEmpty(index))
				throw new ServiceException(MessageHelper.ERROR_NO_INDEX);
			if (StringUtils.isEmpty(type))
				throw new ServiceException(MessageHelper.ERROR_NO_TYPE);

			Response elkResponse = ElasticHelper.getInstance().defineMapping(index, type, new JSONObject(payload));
			String elkResponseStr = IOUtils.toString(elkResponse.getEntity().getContent(), Charsets.UTF_8);

			return new ResponseEntity<>(mapper.readTree(elkResponseStr), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_DEFINEMAPPING, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName))")
	@RequestMapping(value = "index/{config}", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Creates a new index.", notes = "Creates a new index.")
	@ResponseBody
	public ResponseEntity<?> createIndex(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@ApiParam(value = "Config Name") @PathVariable(value = "config") String configName) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_CREATEINDEX, null);
		log.put(MessageHelper.CONST_METHOD, MessageHelper.METHOD_CREATEINDEX);
		log.put(MessageHelper.CONST_OBJECTTYPE, configName);

		try {
			JSONObject config = ConfigurationHelper.getInstance().getConfiguration(configName, authorizationHeader);

			Object document = Configuration.defaultConfiguration().jsonProvider().parse(config.toString());
			String index = JsonPath.read(document, IndexingController.CONST_ELASTIC_INDEX);

			if (StringUtils.isEmpty(index))
				throw new ServiceException(MessageHelper.ERROR_NO_INDEX);

			Response elkResponse = ElasticHelper.getInstance().createIndex(index);
			String elkResponseStr = IOUtils.toString(elkResponse.getEntity().getContent(), Charsets.UTF_8);

			return new ResponseEntity<>(mapper.readTree(elkResponseStr), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_CREATEINDEX, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName))")
	@RequestMapping(value = "index/{config}", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Delete index.", notes = "Delete index.")
	@ResponseBody
	public ResponseEntity<?> delete(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@ApiParam(value = "Config Name") @PathVariable(value = "config") String configName) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_DELETEINDEX, null);
		log.put(MessageHelper.CONST_METHOD, MessageHelper.METHOD_DELETEINDEX);
		log.put(MessageHelper.CONST_OBJECTTYPE, configName);

		try {
			JSONObject config = ConfigurationHelper.getInstance().getConfiguration(configName, authorizationHeader);

			Object document = Configuration.defaultConfiguration().jsonProvider().parse(config.toString());
			String index = JsonPath.read(document, IndexingController.CONST_ELASTIC_INDEX);

			if (StringUtils.isEmpty(index))
				throw new ServiceException(MessageHelper.ERROR_NO_INDEX);

			Response elkResponse = ElasticHelper.getInstance().deleteIndex(index);
			String elkResponseStr = IOUtils.toString(elkResponse.getEntity().getContent(), Charsets.UTF_8);

			return new ResponseEntity<>(mapper.readTree(elkResponseStr), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_DELETEINDEX, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName)) or #config.startsWith('public-')")
	@RequestMapping(value = "config/{config}", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Create or update rules for the specified configuration", notes = "Create or update configuration")
	@ResponseBody
	public ResponseEntity<?> upsertConfigWithPut(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@RequestBody(required = true) String payload,
			@ApiParam(value = "Configuration name") @PathVariable(value = "config") String configName) {
		return upsertConfig(authorizationHeader, payload, configName);
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName)) or #configName.startsWith('public-')")
	@RequestMapping(value = "config/{config}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Create or update rules for the specified configuration", notes = "Create or update configuration")
	@ResponseBody
	public ResponseEntity<?> upsertConfig(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@RequestBody(required = true) String payload,
			@ApiParam(value = "Configuration name") @PathVariable(value = "config") String configName) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_UPSERTCONFIG, configName);

		try {
			// First, check the configuration name
			Pattern p = Pattern.compile(configRegex);
			if (!p.matcher(configName).matches())
				throw new ServiceException(String.format(MessageHelper.ERROR_CONFIGURATION_INVALID, configRegex));

			JSONObject data = new JSONObject(payload);
			ObjectHelper helper = ObjectHelper.getInstance(authorizationHeader);
			if (helper.exists(configName)) {
				helper.updateObject(configName, data);
			} else {
				helper.createObject(data, configName);
			}

			JSONObject json = new JSONObject();
			json.put(MessageHelper.CONST_SUCCESS, true);
			json.put(MessageHelper.CONST_CONFIG, configName);
			return new ResponseEntity<>(mapper.readTree(json.toString()), HttpStatus.OK);

		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_UPSERTCONFIG, log);

			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName)) or #configName.startsWith('public-')")
	@RequestMapping(value = "config/{config}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Get configuration", notes = "Get configuration")
	@ResponseBody
	public ResponseEntity<?> getConfig(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@ApiParam(value = "Configuration name") @PathVariable(value = "config") String configName) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_GETCONFIG, configName);

		try {
			ObjectHelper helper = ObjectHelper.getInstance(authorizationHeader);
			if (!helper.exists(configName))
				throw new ServiceException(MessageHelper.ERROR_CONFIG_DOESNT_EXIST);

			return new ResponseEntity<>(mapper.readTree(helper.getObject(configName).toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_GETCONFIG, log);

			return ErrorHandler.getInstance().handle(e, log);
		}

	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('indexing.'.concat(#configName)) or #configName.startsWith('public-')")
	@RequestMapping(value = "config/{config}", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Delete configuration", notes = "Delete configuration")
	@ResponseBody
	public ResponseEntity<?> deleteConfig(
			@ApiIgnore @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
			@ApiParam(value = "Configuration name") @PathVariable(value = "config") String configName) {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_DELETECONFIG, configName);

		try {
			ObjectHelper helper = ObjectHelper.getInstance(authorizationHeader);
			if (!helper.exists(configName))
				throw new ServiceException(MessageHelper.ERROR_CONFIG_DOESNT_EXIST);

			helper.deleteObject(configName);

			return new ResponseEntity<>(mapper.readTree("{ \"success\" : true }"), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_DELETECONFIG, log);

			return ErrorHandler.getInstance().handle(e, log);
		}

	}

	private void prepareObject(JSONObject object, JSONObject config) throws ServiceException {
		JSONObject mapping = config.getJSONObject("mapping");
		if (mapping != null) {

			// Create new items
			JSONObject elementsToSet = mapping.getJSONObject("$set");
			if (elementsToSet != null) {
				setElements(object, elementsToSet);
			} else
				logger.debug("The $set configuration has not been provided.");

			// Delete unnecessary items
			JSONArray keysToDelete = mapping.getJSONArray("$unset");
			if (keysToDelete != null) {
				unsetElements(object, keysToDelete);
			} else
				logger.debug("The $unset configuration has not been provided.");
		} else
			logger.debug("The mapping configuration has not been provided.");
	}

	private void unsetElements(JSONObject object, JSONArray keysToDelete) {
		for (int i = 0; i < keysToDelete.length(); i++) {
			String key = keysToDelete.getString(i);
			unsetElement(object, key);
		}
	}

	private void unsetElement(JSONObject object, String key) {
		if (key.contains(".")) {
			String parentKey = key.split("\\.")[0];
			String path = key.substring(parentKey.length() + 1);
			if (object.has(parentKey))
				unsetElement(object.getJSONObject(parentKey), path);
		} else {
			if (object.has(key))
				object.remove(key);
		}
	}

	private void setElements(JSONObject object, JSONObject elementsToSet) throws ServiceException {
		for (Object key : elementsToSet.keySet()) {
			setElement(object, key.toString(), elementsToSet.getJSONObject(key.toString()));
		}
	}

	private void setElement(JSONObject object, String key, JSONObject config) throws ServiceException {
		String separator = config.has("separator") ? config.getString("separator") : "";

		JSONObject objectToPopulate = object;
		String fieldName = key;
		if (key.contains(".")) {
			fieldName = key.substring(key.lastIndexOf('.') + 1);
			String path = key.substring(0, key.lastIndexOf('.'));
			objectToPopulate = JSONHelper.getInstance().getOrCreate(object, path);
		}

		// Create the field value
		String fieldValue = createFieldValue(object, config.getJSONArray("fields"), separator);

		// Transform the value
		Object newValue = fieldValue;
		if (config.has("transform")) {
			newValue = QueryBuilder.getInstance().transform(config.getJSONObject("transform"), fieldValue);
		}

		// Save the value
		objectToPopulate.put(fieldName, newValue);
	}

	private String createFieldValue(JSONObject object, JSONArray fields, String separator) {
		StringBuilder sb = new StringBuilder();
		Object document = Configuration.defaultConfiguration().jsonProvider().parse(object.toString());

		for (int i = 0; i < fields.length(); i++) {
			String jsonPath = fields.getString(i);
			try {
				Object valueObj = JsonPath.read(document, jsonPath);
				appendValue(sb, valueObj, separator);
			} catch (PathNotFoundException e) {
				// Do nothing
				logger.debug(e);
			}
		}

		return sb.toString();
	}

	private void appendValue(StringBuilder sb, Object value, String separator) {
		if (value instanceof net.minidev.json.JSONArray) {
			net.minidev.json.JSONArray array = (net.minidev.json.JSONArray) value;
			for (int i = 0; i < array.size(); i++)
				appendValue(sb, array.get(i), separator);
		} else if (value instanceof String && !StringUtils.isEmpty(value)) {
			sb.append(value);
			sb.append(separator);
		}
	}

}