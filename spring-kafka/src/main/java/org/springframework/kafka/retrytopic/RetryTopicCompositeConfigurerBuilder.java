package org.springframework.kafka.retrytopic;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class RetryTopicCompositeConfigurerBuilder {

	private List<RetryTopicComponentConfigurer<?>> configurers;

	private RetryTopicCompositeConfigurerBuilder() {
		this.configurers = new ArrayList<>();
	}

	static RetryTopicCompositeConfigurerBuilder newInstance() {
		return new RetryTopicCompositeConfigurerBuilder();
	}

	public <T> RetryTopicCompositeConfigurerBuilder configure(Class<T> classToConfigure, Consumer<T> configurer) {
		configurers.add(createConfigurer(classToConfigure, configurer, null));
		return this;
	}

	public <T> RetryTopicCompositeConfigurerBuilder configure(Class<T> classToConfigure, Consumer<T> configurer,
															  @Nullable Supplier<String> description) {

		configurers.add(createConfigurer(classToConfigure, configurer, description));
		return this;
	}

	static <T> RetryTopicComponentConfigurer<T> createConfigurer(Class<T> componentToConfigure, Consumer<T> configurer) {
		return createConfigurer(componentToConfigure, configurer, null);
	}

		static <T> RetryTopicComponentConfigurer<T> createConfigurer(Class<T> componentToConfigure, Consumer<T> configurer,
																 @Nullable Supplier<String> description) {
		Assert.notNull(componentToConfigure, "Class to configure cannot be null");
		Assert.notNull(configurer, "Configurer cannot be null");
		Assert.isTrue(RetryTopicComponents.isRetryTopicComponent(componentToConfigure), () -> componentToConfigure.getName()
				+ " is not a registered RetryTopicComponent");
		return new RetryTopicComponentConfigurer<>() {
			@Override
			public Class<T> configures() {
				return componentToConfigure;
			}

			@Override
			public void configure(T component) {
				configurer.accept(component);
			}

			@Override
			public String toString() {
				return description != null
						? description.get()
						: "General Configurer for component " + componentToConfigure + ":  " + configurer;
			}
		};
	}

	public RetryTopicCompositeConfigurer done() {
		return new RetryTopicCompositeConfigurer(this.configurers);
	}
}
