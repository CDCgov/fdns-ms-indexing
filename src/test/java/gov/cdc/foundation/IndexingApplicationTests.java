package gov.cdc.foundation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.assertj.core.api.AssertProvider;
import org.hamcrest.CoreMatchers;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.boot.test.json.JsonContent;
import org.springframework.boot.test.json.JsonContentAssert;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;

import gov.cdc.foundation.helper.ConfigurationHelper;
import gov.cdc.helper.ObjectHelper;
import org.springframework.web.client.HttpClientErrorException;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, properties = {
		"logging.fluentd.host=fluentd",
		"logging.fluentd.port=24224",
		"elastic.host=elastic",
		"elastic.port=9200",
		"elastic.protocol=http",
		"proxy.hostname=",
		"security.oauth2.resource.user-info-uri=",
		"security.oauth2.protected=",
		"security.oauth2.client.client-id=",
		"security.oauth2.client.client-secret=",
		"ssl.verifying.disable=false" })
@AutoConfigureMockMvc
public class IndexingApplicationTests {

	@Autowired
	private TestRestTemplate restTemplate;
	private JacksonTester<JsonNode> json;
	private String baseUrlPath = "/api/1.0/";

	private String configurationProfileName = "test";

	private List<String> objectIds;
	private int NB_OF_ITEMS_TO_CREATE = 20;

	@Before
	public void setup() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		JacksonTester.initFields(this, objectMapper);	
		
		// Define the object URL
		System.setProperty("OBJECT_URL", "http://fdns-ms-object:8083");
		
		// Add the config and load it
		String payload = getConfig("test.json");
		ConfigurationHelper.getInstance().createConfiguration("test", payload, null);
		JSONObject config = ConfigurationHelper.getInstance().getConfiguration("test", null);

		// Delete the collection
		deleteCollection(config);

		// Create some items in the database
		objectIds = new ArrayList<>();
		for (int i = 0; i < NB_OF_ITEMS_TO_CREATE; i++) {
			String value = String.format("%02d", i);
			objectIds.add(createObject(value));
		}
	}

	private String createObject(String objectId) throws Exception {
		// Load test configuration
		JSONObject config = ConfigurationHelper.getInstance().getConfiguration(configurationProfileName, null);
		Object document = Configuration.defaultConfiguration().jsonProvider().parse(config.toString());
		String database = JsonPath.read(document, "$.mongo.database");
		String collection = JsonPath.read(document, "$.mongo.collection");

		// Build JSON Object
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		JSONObject data = new JSONObject();
		data.put("value", objectId);
		data.put("other", new JSONObject("{ 'useless' : 0 }"));
		data.put("date", sdf.format(Calendar.getInstance().getTime()));

		JSONObject response = ObjectHelper.getInstance().createObject(data, objectId, database, collection);
		
		// Check the body
		assertThat(forJson(response.toString())).hasJsonPathValue("@._id");

		// Extract the id and returns it
		return response.getString("_id");
	}

	private void deleteCollection(JSONObject config) throws Exception {
		// Parse test configuration
		Object document = Configuration.defaultConfiguration().jsonProvider().parse(config.toString());
		String database = JsonPath.read(document, "$.mongo.database");
		String collection = JsonPath.read(document, "$.mongo.collection");

		//if it's already been deleted we'll get a 404, so we'll absorb that exception
		try {
			ObjectHelper.getInstance().deleteCollection(database, collection);
		}catch (HttpClientErrorException e){
			if(!e.getStatusCode().equals(HttpStatus.NOT_FOUND)){
				throw e;
			}
		}
	}

	@Test
	public void indexPage() {
		ResponseEntity<String> response = this.restTemplate.getForEntity("/", String.class);
		assertThat(response.getStatusCodeValue()).isEqualTo(200);
		assertThat(response.getBody(), CoreMatchers.containsString("FDNS Indexing Microservice"));
	}

	@Test
	public void indexAPI() {
		ResponseEntity<String> response = this.restTemplate.getForEntity(baseUrlPath, String.class);
		assertThat(response.getStatusCodeValue()).isEqualTo(200);
		assertThat(response.getBody(), CoreMatchers.containsString("version"));
	}
	
	@Test
	public void manageIndexes() throws IOException {
		// Delete index that does not exists
		ResponseEntity<JsonNode> response = this.restTemplate.exchange(baseUrlPath + "/index/{type}", HttpMethod.DELETE, null, JsonNode.class, configurationProfileName);
		//grab response from second run to make sure that the index doesn't exist
		response = this.restTemplate.exchange(baseUrlPath + "/index/{type}", HttpMethod.DELETE, null, JsonNode.class, configurationProfileName);
		JsonContent<JsonNode> body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").isEqualTo("This index doesn't exist.");
		
		// Delete index with a wrong type
		response = this.restTemplate.exchange(baseUrlPath + "/index/{type}", HttpMethod.DELETE, null, JsonNode.class, "_unknown_");
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").contains("The configuration for the following object type doesn't exist");
		
		// Create index with a wrong type
		response = this.restTemplate.exchange(baseUrlPath + "/index/{type}", HttpMethod.PUT, null, JsonNode.class, "_unknown_");
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").contains("The configuration for the following object type doesn't exist");
		
		// Create index 
		response = this.restTemplate.exchange(baseUrlPath + "/index/{type}", HttpMethod.PUT, null, JsonNode.class, configurationProfileName);
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathBooleanValue("@.acknowledged");
		assertThat(body).extractingJsonPathBooleanValue("@.acknowledged").isEqualTo(true);
		
		// Create index (a 2nd time)
		response = this.restTemplate.exchange(baseUrlPath + "/index/{type}", HttpMethod.PUT, null, JsonNode.class, configurationProfileName);
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").isEqualTo("This index already exists.");
		
		// Delete index 
		response = this.restTemplate.exchange(baseUrlPath + "/index/{type}", HttpMethod.DELETE, null, JsonNode.class, configurationProfileName);
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathBooleanValue("@.acknowledged");
		assertThat(body).extractingJsonPathBooleanValue("@.acknowledged").isEqualTo(true);
		
		// Delete index (a 2nd time)
		response = this.restTemplate.exchange(baseUrlPath + "/index/{type}", HttpMethod.DELETE, null, JsonNode.class, configurationProfileName);
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").isEqualTo("This index doesn't exist.");
	}
	
	@Test
	public void defineMapping() throws IOException {
		// Create headers
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.setContentType(MediaType.APPLICATION_JSON);
		httpHeaders.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
		// Create empty payload
		HttpEntity<String> emptyMappingPayload = new HttpEntity<>((new JSONObject()).toString(), httpHeaders);
		// Create valid payload
		HttpEntity<String> validMappingPayload = new HttpEntity<>("{ 'properties': { 'value' : { 'type' : 'string' } } } ", httpHeaders);
		// Create new version of the valid payload
		HttpEntity<String> validMappingPayload_v2 = new HttpEntity<>("{ 'properties': { 'value' : { 'type' : 'date' } } } ", httpHeaders);
		
		// Create mapping without index
		ResponseEntity<JsonNode> response = this.restTemplate.exchange(baseUrlPath + "/mapping/{type}", HttpMethod.POST, emptyMappingPayload, JsonNode.class, configurationProfileName);
		JsonContent<JsonNode> body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").isEqualTo("This index doesn't exist.");
		
		// Create mapping with a wrong type
		response = this.restTemplate.exchange(baseUrlPath + "/mapping/{type}", HttpMethod.POST, emptyMappingPayload, JsonNode.class,  "_unknown_");
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").contains("The configuration for the following object type doesn't exist");
		
		// Create index
		response = this.restTemplate.exchange(baseUrlPath + "/index/{type}", HttpMethod.PUT, null, JsonNode.class, configurationProfileName);
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathBooleanValue("@.acknowledged");
		assertThat(body).extractingJsonPathBooleanValue("@.acknowledged").isEqualTo(true);

		// Create mapping with a wrong mapping
		response = this.restTemplate.exchange(baseUrlPath + "/mapping/{type}", HttpMethod.POST, emptyMappingPayload, JsonNode.class, configurationProfileName);
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").isEqualTo("malformed mapping no root object found");
		
		// Create mapping
		response = this.restTemplate.exchange(baseUrlPath + "/mapping/{type}", HttpMethod.POST, validMappingPayload, JsonNode.class, configurationProfileName);
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
		assertThat(body).hasJsonPathBooleanValue("@.acknowledged");
		assertThat(body).extractingJsonPathBooleanValue("@.acknowledged").isEqualTo(true);
		
		// Create mapping (but changing the type)
		response = this.restTemplate.exchange(baseUrlPath + "/mapping/{type}", HttpMethod.POST, validMappingPayload_v2, JsonNode.class, configurationProfileName);
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").isEqualTo("mapper [value] of different type, current_type [text], merged_type [date]");

		// Delete index 
		response = this.restTemplate.exchange(baseUrlPath + "/index/{type}", HttpMethod.DELETE, null, JsonNode.class, configurationProfileName);
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathBooleanValue("@.acknowledged");
		assertThat(body).extractingJsonPathBooleanValue("@.acknowledged").isEqualTo(true);
	}
	
	@Test
	public void manageObjects() throws IOException {
		// Index object with a wrong type
		ResponseEntity<JsonNode> response = this.restTemplate.exchange(baseUrlPath + "/index/{type}/{id}", HttpMethod.POST, null, JsonNode.class, "_unknown_", "_unknown_");
		JsonContent<JsonNode> body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").contains("The configuration for the following object type doesn't exist");
		
		// Index object with a wrong id
		response = this.restTemplate.exchange(baseUrlPath + "/index/{type}/{id}", HttpMethod.POST, null, JsonNode.class, configurationProfileName, "_unknown_");
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").contains("The following object doesn't exist.");
		
		// Index all objects
		for (int i = 0; i < NB_OF_ITEMS_TO_CREATE; i++) {
			String id = String.format("%02d", i);
			response = this.restTemplate.exchange(baseUrlPath + "/index/{type}/{id}", HttpMethod.POST, null, JsonNode.class, configurationProfileName, id);
			body = this.json.write(response.getBody());
			assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
			assertThat(body).hasJsonPathBooleanValue("@.elk.created");
			assertThat(body).extractingJsonPathBooleanValue("@.elk.created").isEqualTo(true);
			assertThat(body).hasJsonPathNumberValue("@.elk._version");
			assertThat(body).extractingJsonPathNumberValue("@.elk._version").isEqualTo(1);
			assertThat(body).hasJsonPathStringValue("@.elk.result");
			assertThat(body).extractingJsonPathStringValue("@.elk.result").isEqualTo("created");
			assertThat(body).hasJsonPathStringValue("@.elk._id");
			assertThat(body).extractingJsonPathStringValue("@.elk._id").isEqualTo(id);
		}
		
		// Index all objects (a 2nd time)
		for (int i = 0; i < NB_OF_ITEMS_TO_CREATE; i++) {
			String id = String.format("%02d", i);
			response = this.restTemplate.exchange(baseUrlPath + "/index/{type}/{id}", HttpMethod.POST, null, JsonNode.class, configurationProfileName, id);
			body = this.json.write(response.getBody());
			assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
			assertThat(body).hasJsonPathBooleanValue("@.elk.created");
			assertThat(body).extractingJsonPathBooleanValue("@.elk.created").isEqualTo(false);
			assertThat(body).hasJsonPathNumberValue("@.elk._version");
			assertThat(body).extractingJsonPathNumberValue("@.elk._version").isEqualTo(2);
			assertThat(body).hasJsonPathStringValue("@.elk.result");
			assertThat(body).extractingJsonPathStringValue("@.elk.result").isEqualTo("updated");
			assertThat(body).hasJsonPathStringValue("@.elk._id");
			assertThat(body).extractingJsonPathStringValue("@.elk._id").isEqualTo(id);
		}
		
		// Get all objects
		for (int i = 0; i < NB_OF_ITEMS_TO_CREATE; i++) {
			String id = String.format("%02d", i);
			response = this.restTemplate.exchange(baseUrlPath + "/get/{type}/{id}", HttpMethod.GET, null, JsonNode.class, configurationProfileName, id);
			body = this.json.write(response.getBody());
			assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
			assertThat(body).hasJsonPathStringValue("@._index");
			assertThat(body).extractingJsonPathStringValue("@._index").isEqualTo("test");
			assertThat(body).hasJsonPathStringValue("@._type");
			assertThat(body).extractingJsonPathStringValue("@._type").isEqualTo("junit");
			assertThat(body).hasJsonPathStringValue("@._id");
			assertThat(body).extractingJsonPathStringValue("@._id").isEqualTo(id);
			assertThat(body).hasJsonPathNumberValue("@._version");
			assertThat(body).extractingJsonPathNumberValue("@._version").isEqualTo(2);
			assertThat(body).hasJsonPathBooleanValue("@.found");
			assertThat(body).extractingJsonPathBooleanValue("@.found").isEqualTo(true);
			assertThat(body).hasJsonPathStringValue("@._source.value");
			assertThat(body).extractingJsonPathStringValue("@._source.value").isEqualTo(id);
		}
		
		// Search object with a wrong type
		response = this.restTemplate.exchange(baseUrlPath + "/search/{type}", HttpMethod.POST, null, JsonNode.class, "_unknown_");
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		assertThat(body).hasJsonPathStringValue("@.message");
		assertThat(body).extractingJsonPathStringValue("@.message").contains("The configuration for the following object type doesn't exist");
		
		// Search all objects
		response = this.restTemplate.exchange(baseUrlPath + "/search/{type}", HttpMethod.POST, null, JsonNode.class, configurationProfileName);
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathNumberValue("@.hits.total");
		assertThat(body).extractingJsonPathNumberValue("@.hits.total").isEqualTo(20);
		
		// Search unique object
		response = this.restTemplate.exchange(baseUrlPath + "/search/{type}?query={query}", HttpMethod.POST, null, JsonNode.class, configurationProfileName, "val:10");
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathNumberValue("@.hits.total");
		assertThat(body).extractingJsonPathNumberValue("@.hits.total").isEqualTo(1);
		
		// Search unique object with hydration
		response = this.restTemplate.exchange(baseUrlPath + "/search/{type}?query={query}&hydrate=true", HttpMethod.POST, null, JsonNode.class, configurationProfileName, "val:10");
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathNumberValue("@.hits.total");
		assertThat(body).extractingJsonPathNumberValue("@.hits.total").isEqualTo(1);
		
		// Search two objects
		response = this.restTemplate.exchange(baseUrlPath + "/search/{type}?query={query}", HttpMethod.POST, null, JsonNode.class, configurationProfileName, "val:10 val:11");
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathNumberValue("@.hits.total");
		assertThat(body).extractingJsonPathNumberValue("@.hits.total").isEqualTo(2);
		
		// Search two objects with OR
		response = this.restTemplate.exchange(baseUrlPath + "/search/{type}?query={query}", HttpMethod.POST, null, JsonNode.class, configurationProfileName, "10 OR 11");
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathNumberValue("@.hits.total");
		assertThat(body).extractingJsonPathNumberValue("@.hits.total").isEqualTo(2);
		
		// Search objests with an `everything` filter
		response = this.restTemplate.exchange(baseUrlPath + "/search/{type}?query={query}", HttpMethod.POST, null, JsonNode.class, configurationProfileName, "val:10 hello");
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathNumberValue("@.hits.total");
		assertThat(body).extractingJsonPathNumberValue("@.hits.total").isEqualTo(0);
		
		// Search objects with date
		SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy");
		response = this.restTemplate.exchange(baseUrlPath + "/search/{type}?query={query}", HttpMethod.POST, null, JsonNode.class, configurationProfileName, "start:" + sdf.format(Calendar.getInstance().getTime()));
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathNumberValue("@.hits.total");
		assertThat(body).extractingJsonPathNumberValue("@.hits.total").isEqualTo(NB_OF_ITEMS_TO_CREATE);

		// Delete index 
		response = this.restTemplate.exchange(baseUrlPath + "/index/{type}", HttpMethod.DELETE, null, JsonNode.class, configurationProfileName);
		body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathBooleanValue("@.acknowledged");
		assertThat(body).extractingJsonPathBooleanValue("@.acknowledged").isEqualTo(true);
	}
	
	private AssertProvider<JsonContentAssert> forJson(final String json) {
		return () -> new JsonContentAssert(IndexingApplicationTests.class, json);
	}
	
	private InputStream getResource(String path) throws IOException {
		return getClass().getClassLoader().getResourceAsStream(path);
	}

	private String getResourceAsString(String path) throws IOException {
		return IOUtils.toString(getResource(path));
	}
	
	private String getConfig(String filename) throws IOException {
		return getResourceAsString("config/" + filename);
	}

}
