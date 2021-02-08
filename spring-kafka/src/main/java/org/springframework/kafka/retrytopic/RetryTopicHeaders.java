package org.springframework.kafka.retrytopic;

import java.time.format.DateTimeFormatter;

/**
 * @author tomazlemos
 * @since 24/01/21
 */
public class RetryTopicHeaders {


	/**
	 * The default kafka header for the timestamp.
	 */
	public static final String DEFAULT_HEADER_BACKOFF_TIMESTAMP = "retry_topic-backoff-timestamp";
	public static final String DEFAULT_HEADER_ATTEMPTS = "retry_topic-attempts";
	public static final DateTimeFormatter DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

}
