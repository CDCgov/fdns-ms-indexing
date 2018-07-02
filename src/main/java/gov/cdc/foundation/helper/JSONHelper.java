package gov.cdc.foundation.helper;

import org.json.JSONObject;

public class JSONHelper {

	private static JSONHelper me;

	public static JSONHelper getInstance() {
		if (me == null)
			me = new JSONHelper();
		return me;
	}

	public JSONObject getOrCreate(JSONObject parent, String key) {
		if (key.contains(".")) {
			String parentKey = key.split("\\.")[0];
			String path = key.substring(parentKey.length() + 1);
			if (!parent.has(parentKey))
				parent.put(parentKey, new JSONObject());
			return getOrCreate(parent.getJSONObject(parentKey), path);
		} else {
			if (!parent.has(key))
				parent.put(key, new JSONObject());
			return parent.getJSONObject(key);
		}
	}
}
