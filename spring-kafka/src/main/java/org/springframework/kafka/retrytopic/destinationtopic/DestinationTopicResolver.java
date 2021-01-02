package org.springframework.kafka.retrytopic.destinationtopic;

/**
 *
 * Contains the methods used by the {@link org.springframework.kafka.listener.DeadLetterPublishingRecoverer} to
 * resolve the destination topics and backoff header.
 *
 * @author Tomaz Fernandes
 * @since 2.7.0
 */
public interface DestinationTopicResolver {

	String resolveDestinationFor(String topic);
	String resolveDltDestinationFor(String topic);
	String resolveDestinationNextExecutionTime(String topic);

}
