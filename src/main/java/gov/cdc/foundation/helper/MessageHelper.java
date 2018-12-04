package gov.cdc.foundation.helper;

import java.util.HashMap;
import java.util.Map;

import gov.cdc.helper.AbstractMessageHelper;

public class MessageHelper extends AbstractMessageHelper {

	public static final String CONST_OBJECTTYPE = "objectType";
	public static final String CONST_OBJECTID = "objectId";
	public static final String CONST_CONFIG = "config";
	public static final String CONST_TYPE = "type";
	public static final String CONST_ROOT_CAUSE = "root_cause";

	public static final String METHOD_INDEX = "index";
	public static final String METHOD_INDEXOBJECT = "indexObject";
	public static final String METHOD_INDEXBULKOBJECTS = "indexBulkObjects";
	public static final String METHOD_GETOBJECT = "getObject";
	public static final String METHOD_SEARCHOBJECT = "searchObjects";
	public static final String METHOD_DEFINEMAPPING = "defineMapping";
	public static final String METHOD_CREATEINDEX = "createIndex";
	public static final String METHOD_DELETEINDEX = "deleteIndex";
	public static final String METHOD_SCROLL = "scroll";
	public static final String METHOD_INDEXALL = "indexAll";
	public static final String METHOD_UPSERTCONFIG = "upsertConfig";
	public static final String METHOD_GETCONFIG = "getConfig";
	public static final String METHOD_DELETECONFIG = "deleteConfig";

	public static final String ERROR_CONFIGURATION_INVALID = "The configuration name is not valid, it must match the following expression: %s";
	public static final String ERROR_CONFIG_DOESNT_EXIST = "This configuration doesn't exist.";
	public static final String ERROR_NO_DATABASE = "The database has not been provided in the configuration file.";
	public static final String ERROR_NO_COLLECTION = "The collection has not been provided in the configuration file.";
	public static final String ERROR_NO_INDEX = "The index has not been provided in the configuration file.";
	public static final String ERROR_INDEX_DOESNT_EXIST = "This index doesn't exist.";
	public static final String ERROR_SCROLL_IDENTIFIER_DOESNT_EXIST = "This scroll identifier doesn't exist";
	public static final String ERROR_INDEX_ALREADY_EXIST = "This index already exists.";
	public static final String ERROR_NO_TYPE = "The type has not been provided in the configuration file.";
	public static final String ERROR_NO_OBJECT = "The following object doesn't exist.";
	public static final String ERROR_BULK_MAX = "The bulk indexing processs accepts a maximum of 100 ids.";

	public static final String EXCEPTION_ILLEGAL_ARGUMENT = "illegal_argument_exception";
	public static final String EXCEPTION_PARSE = "parse_exception";

	private MessageHelper() {
		throw new IllegalAccessError("Helper class");
	}
	
	public static Map<String, Object> initializeLog(String method, String config) {
		Map<String, Object> log = new HashMap<>();
		log.put(MessageHelper.CONST_METHOD, method);
		if (config != null)
			log.put(MessageHelper.CONST_CONFIG, config);
		return log;
	}

}