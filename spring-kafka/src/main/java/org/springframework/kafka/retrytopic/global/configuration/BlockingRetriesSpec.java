package org.springframework.kafka.retrytopic.global.configuration;

import org.springframework.kafka.retrytopic.ListenerContainerFactoryConfigurer;
import org.springframework.kafka.retrytopic.RetryTopicCompositeConfigurerBuilder;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;

import java.util.Arrays;

public class BlockingRetriesSpec<P extends RetryTopicGlobalSpec> extends AbstractConfigurationSpec<P> {

	public BlockingRetriesSpec(P previousStep, RetryTopicCompositeConfigurerBuilder builder) {
		super(previousStep, builder);
	}

	@SafeVarargs
	@SuppressWarnings("varargs")
	public final BlockingRetriesSpec<P> retryOnExceptionTypes(Class<? extends Exception>... exceptionTypes) {
		Assert.notEmpty(exceptionTypes, "Exception types should not be null or empty");
		addConfigurer(ListenerContainerFactoryConfigurer.class,
				lcfc -> lcfc.setBlockingRetryableExceptions(exceptionTypes), () -> "retry on exception types: "
						+ Arrays.toString(exceptionTypes));
		return this;
	}

	public BlockingRetriesSpec<P> backOffPolicy(BackOff backOff) {
		Assert.notNull(backOff, "BackOff should not be null");
		addConfigurer(ListenerContainerFactoryConfigurer.class,
				lcfc -> lcfc.setBlockingRetriesBackOff(backOff), () -> "with back off policy: " + backOff);
		return this;
	}
}
