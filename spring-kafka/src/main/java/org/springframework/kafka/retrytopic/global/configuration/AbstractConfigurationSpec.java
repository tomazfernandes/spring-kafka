package org.springframework.kafka.retrytopic.global.configuration;

import org.springframework.kafka.retrytopic.RetryTopicCompositeConfigurerBuilder;

import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class AbstractConfigurationSpec<P extends RetryTopicGlobalSpec> implements RetryTopicGlobalSpec {

	private final P previousStep;
	private final RetryTopicCompositeConfigurerBuilder holder;

	public AbstractConfigurationSpec(P previousStep, RetryTopicCompositeConfigurerBuilder holder) {
		this.previousStep = previousStep;
		this.holder = holder;
	}

	protected <T> void addConfigurer(Class<T> clazz, Consumer<T> configurer, Supplier<String> description) {
		this.holder.configure(clazz, configurer, description);
	}

	public P and() {
		return previousStep;
	}
}
