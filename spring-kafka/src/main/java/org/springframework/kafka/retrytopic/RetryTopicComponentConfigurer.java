package org.springframework.kafka.retrytopic;

public interface RetryTopicComponentConfigurer<T> {

	Class<T> configures();
	void configure(T component);

}
