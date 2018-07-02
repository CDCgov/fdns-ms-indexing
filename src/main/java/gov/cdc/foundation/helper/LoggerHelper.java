package gov.cdc.foundation.helper;

import java.util.Map;

import org.apache.log4j.Logger;
import org.fluentd.logger.FluentLogger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class LoggerHelper {

	private static FluentLogger fluent;

	private static final Logger logger = Logger.getLogger(LoggerHelper.class);

	private static String prefix;
	private static String host;
	private static int port;

	private LoggerHelper(@Value("${logging.fluentd.host}") String host, @Value("${logging.fluentd.port}") int port, @Value("${logging.fluentd.prefix}") String prefix) {
		logger.debug("Creating logger helper...");
		LoggerHelper.host = host;
		LoggerHelper.prefix = prefix;
		LoggerHelper.port = port;
	}

	public static void log(String action, Map<String, Object> data) {
		FluentLogger myLogger = getLogger();
		if (myLogger != null)
			myLogger.log(action, data);
	}

	private static FluentLogger getLogger() {
		if (fluent == null)
			try {
				fluent = FluentLogger.getLogger(prefix, host, port);
			} catch (NoClassDefFoundError e) {
				logger.error(e);
			}
		return fluent;
	}

}
