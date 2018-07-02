package gov.cdc.foundation.helper;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import gov.cdc.helper.ObjectHelper;

import gov.cdc.helper.common.ServiceException;

public class ConfigurationHelper {

	private static final Logger logger = Logger.getLogger(ConfigurationHelper.class);

	private static ConfigurationHelper me = null;

	public static ConfigurationHelper getInstance() {
		if (me == null)
			me = new ConfigurationHelper();
		return me;
	}

	public JSONObject getConfiguration(String config, String authorizationHeader) throws ServiceException {
		try {
			ObjectHelper helper = ObjectHelper.getInstance(authorizationHeader);
			if (!helper.exists(config))
				throw new ServiceException(
					"The configuration for the following object type doesn't exist: " + config);

			return helper.getObject(config);
		} catch (Exception e) {
			logger.error(e);
			throw new ServiceException(e);
		}
	}
	
	public void createConfiguration(String config, String payload, String authorizationHeader) throws ServiceException {
		try {
			JSONObject data = new JSONObject(payload);
			ObjectHelper helper = ObjectHelper.getInstance(authorizationHeader);
			if (helper.exists(config)) {
				helper.updateObject(config, data);
			} else {
				helper.createObject(data, config);
			}
		} catch (Exception e) {
			logger.error(e);;
			throw new ServiceException(e);
		}
	}
}
