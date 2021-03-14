package org.springframework.kafka.retrytopic;

import java.util.List;

/**
 *
 * Provides methods to store and retrieve {@link DestinationTopic} instances.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 */
public interface DestinationTopicContainer {

	/**
	 * Adds the provided destination topics to the container.
	 * @param destinationTopics
	 */
	void addDestinationTopics(List<DestinationTopic> destinationTopics);

	/**
	 * Returns the DestinationTopic instance registered for that topic.
	 * @param topic the topic name of the DestinationTopic to be returned.
	 * @return the DestinationTopic instance registered for that topic.
	 */
	DestinationTopic getDestinationTopicByName(String topicName);

}
