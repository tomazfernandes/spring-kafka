package org.springframework.kafka.retrytopic.global.configuration;

import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory;
import org.springframework.kafka.retrytopic.ListenerContainerFactoryConfigurer;
import org.springframework.kafka.retrytopic.RetryTopicCompositeConfigurerBuilder;
import org.springframework.util.Assert;

import java.util.function.Consumer;

public class CustomizersSpec<P extends RetryTopicGlobalSpec> extends AbstractConfigurationSpec<P> {

	public CustomizersSpec(P previousStep, RetryTopicCompositeConfigurerBuilder builder) {
		super(previousStep, builder);
	}

	public CustomizersSpec<P> listenerContainer(Consumer<ConcurrentMessageListenerContainer<?, ?>> customizer) {
		Assert.notNull(customizer, "Customizer should not be null");
		addConfigurer(ListenerContainerFactoryConfigurer.class,
				lcfc -> lcfc.setContainerCustomizer(customizer), () -> "ConcurrentMessageListenerContainer customizer: " + customizer);
		return this;
	}

	public CustomizersSpec<P> commonErrorHandler(Consumer<CommonErrorHandler> customizer) {
		Assert.notNull(customizer, "Customizer should not be null");
		addConfigurer(ListenerContainerFactoryConfigurer.class,
				lcfc -> lcfc.setErrorHandlerCustomizer(customizer), () -> "CommonErrorHandler customizer: " + customizer);
		return this;
	}

	public CustomizersSpec<P> deadLetterPublishingRecoverer(Consumer<DeadLetterPublishingRecoverer> customizer) {
		Assert.notNull(customizer, "Customizer should not be null");
		addConfigurer(DeadLetterPublishingRecovererFactory.class,
				dlprf -> dlprf.setDeadLetterPublishingRecovererCustomizer(customizer), () -> "DeadLetterPublishingRecoverer customizer: " + customizer);
		return this;
	}
}
