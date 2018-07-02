package gov.cdc.foundation.helper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import gov.cdc.helper.common.ServiceException;

@Component
public class QueryBuilder {

	private static final Logger logger = Logger.getLogger(QueryBuilder.class);

	private static QueryBuilder instance;

	private static final String CONST_REGEX = "regex";

	public static QueryBuilder getInstance() {
		if (instance == null)
			instance = new QueryBuilder();
		return instance;
	}

	public JSONObject parse(JSONObject config, String query) throws ServiceException {
		logger.debug("Query: " + query);

		if (StringUtils.isEmpty(query))
			return null;

		JSONObject queryObj = new JSONObject();
		JSONObject boolObj = JSONHelper.getInstance().getOrCreate(queryObj, "bool");

		// Get filters from config
		List<JSONObject> postFilters = new ArrayList<>();
		JSONObject filters = config.getJSONObject("filters");
		String processingQuery = (new StringBuilder(query)).toString();
		for (Object key : filters.keySet()) {
			JSONObject filter = filters.getJSONObject((String) key);
			// If it's a traditional filter
			if (filter.has(QueryBuilder.CONST_REGEX) && filter.get(QueryBuilder.CONST_REGEX) instanceof String) {
				logger.debug("Applying filter: " + key);
				JSONObject elkFilter = parseFilter(filter, query);
				append(boolObj, elkFilter);
				logger.debug(elkFilter);
				processingQuery = processingQuery.replaceAll((String) filter.get(QueryBuilder.CONST_REGEX), "");
			} else {
				// If it's a filter that applies on the rest of the query
				// Save it for later
				postFilters.add(filter);
			}
		}

		String remainingQuery = processingQuery.trim();
		if (!StringUtils.isEmpty(remainingQuery))
			for (JSONObject filter : postFilters) {
				filter.put("value", processingQuery.trim());
				JSONObject elkFilter = parseFilter(filter, query);
				logger.debug(elkFilter);
				append(boolObj, elkFilter);
			}
		return queryObj;
	}

	private JSONObject parseFilter(JSONObject filter, String query) throws ServiceException {
		// Get the list of values
		List<String> values = extractValues(filter, query);

		if (values.isEmpty())
			return null;

		List<JSONObject> elkFilters = new ArrayList<>();
		String clause = filter.getString("clause");
		String queryType = filter.getString("queryType");

		for (String value : values) {
			// Check if we need to transform the value
			Object newValue = value;
			if (filter.has("transform"))
				newValue = transform(filter.getJSONObject("transform"), value);

			// Create the query type
			JSONObject queryObj;
			if ("multi_match".equalsIgnoreCase(queryType))
				queryObj = createMultiMatchQuery(filter, newValue);
			else if ("range".equalsIgnoreCase(queryType))
				queryObj = createRangeQuery(filter, newValue);
			else
				throw new ServiceException("The following query type is not supported: " + queryType);

			// Build the clause
			JSONObject clauseObj = new JSONObject();
			clauseObj.put(queryType, queryObj);

			elkFilters.add(clauseObj);
		}

		if (elkFilters.size() == 1) {
			JSONObject parentQuery = new JSONObject();

			// Create the clause
			JSONArray clauseObjects = new JSONArray();
			parentQuery.put(clause, clauseObjects);
			clauseObjects.put(elkFilters.get(0));

			return parentQuery;
		} else {
			// We need to build a SHOULD query
			JSONObject parentQuery = new JSONObject();
			JSONArray shouldItems = new JSONArray();
			parentQuery.put(clause, shouldItems);

			JSONArray filters = new JSONArray();
			for (JSONObject elkFilter : elkFilters) {
				filters.put(elkFilter);
			}
			JSONObject shouldItem = new JSONObject();
			shouldItem.put("bool", new JSONObject());
			shouldItem.getJSONObject("bool").put("should", filters);
			shouldItems.put(shouldItem);

			return parentQuery;
		}
	}

	private List<String> extractValues(JSONObject filter, String query) {
		List<String> values = new ArrayList<>();
		if (filter.has(QueryBuilder.CONST_REGEX) && filter.get(QueryBuilder.CONST_REGEX) instanceof String) {
			String regex = filter.getString(QueryBuilder.CONST_REGEX);
			Pattern p = Pattern.compile(regex);
			Matcher m = p.matcher(query);
			int group = filter.getInt("regexGroup");
			while (m.find()) {
				values.add(m.group(group));
			}
			if (group == 0 && !values.isEmpty())
				values = Collections.singletonList(values.get(0));
		} else {
			values.add(filter.getString("value"));
		}
		return values;
	}

	public Object transform(JSONObject config, String value) throws ServiceException {
		if ("date".equalsIgnoreCase(config.getString("from")))
			return transformDate(config, value);
		else if ("string".equalsIgnoreCase(config.getString("from")))
			return transformString(config, value);
		else
			throw new ServiceException("Impossible to transform the following type: " + config.getString("from"));
	}

	private Object transformDate(JSONObject config, String value) throws ServiceException {
		if ("timestamp".equalsIgnoreCase(config.getString("to"))) {
			if (!StringUtils.isEmpty(value)) {
				String format = config.getString("format");
				SimpleDateFormat df = new SimpleDateFormat(format);
				try {
					Date date = df.parse(value);
					return date.getTime();
				} catch (ParseException e) {
					logger.error(e);
					throw new ServiceException(e);
				}
			} else
				return null;
		} else
			throw new ServiceException("Impossible to transform a date to the following type: " + config.getString("to"));
	}

	private Object transformString(JSONObject config, String value) throws ServiceException {
		if ("string".equalsIgnoreCase(config.getString("to"))) {
			String newValue = (new StringBuilder(value)).toString();
			if (config.has(QueryBuilder.CONST_REGEX)) {
				String regex = config.getString(QueryBuilder.CONST_REGEX);
				String replacement = config.has("replacement") ? config.getString("replacement") : "";
				return newValue.replaceAll(regex, replacement);
			}
			return newValue;
		} else
			throw new ServiceException("Impossible to transform a string to the following type: " + config.getString("to"));
	}

	private JSONObject createMultiMatchQuery(JSONObject filter, Object value) {
		JSONObject query = new JSONObject();
		query.put("query", value);
		query.put("fields", filter.getJSONArray("fields"));
		return query;
	}

	private JSONObject createRangeQuery(JSONObject filter, Object value) {
		JSONObject query = new JSONObject();
		JSONObject operator = new JSONObject();
		operator.put(filter.getString("operator"), value);
		query.put(filter.getString("field"), operator);
		return query;
	}

	private void append(JSONObject parent, JSONObject child) throws ServiceException {
		if (child != null) {
			String clause = (String) child.keySet().toArray()[0];
			if (parent.has(clause)) {
				addAll(parent, child, clause);
			} else
				parent.put(clause, child.get(clause));
		}
	}

	private void addAll(JSONObject parent, JSONObject child, String clause) throws ServiceException {
		Object clauses = parent.get(clause);
		if (clauses instanceof JSONArray) {
			JSONArray newClauses = child.getJSONArray(clause);
			for (int i = 0; i < newClauses.length(); i++)
				((JSONArray) clauses).put(newClauses.get(i));
		} else
			throw new ServiceException("Unsupported append operation.");
	}

}
