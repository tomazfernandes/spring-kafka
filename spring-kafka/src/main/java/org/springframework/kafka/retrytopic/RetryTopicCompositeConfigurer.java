package org.springframework.kafka.retrytopic;

import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Collections;

public class RetryTopicCompositeConfigurer {

	private final Collection<RetryTopicComponentConfigurer<?>> configurers;

	public RetryTopicCompositeConfigurer(Collection<RetryTopicComponentConfigurer<?>> configurers) {
		Assert.notNull(configurers, "Configurers cannot be null");
		Assert.notEmpty(configurers, "No configurers were provided");
		this.configurers = Collections.unmodifiableCollection(configurers);
	}

	public Collection<RetryTopicComponentConfigurer<?>> getConfigurers() {
		return configurers;
	}
}
