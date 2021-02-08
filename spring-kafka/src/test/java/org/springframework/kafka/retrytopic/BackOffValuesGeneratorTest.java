package org.springframework.kafka.retrytopic;

import org.junit.jupiter.api.Test;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.NoBackOffPolicy;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BackOffValuesGeneratorTest {

	@Test
	void shouldGenerateWithDefaultValues() {
		// Default MAX_ATTEMPTS = 3
		// Default Policy = FixedBackoffPolicy

		// setup
		BackOffValuesGenerator backOffValuesGenerator = new BackOffValuesGenerator(-1, null);

		// when
		List<Long> backOffValues = backOffValuesGenerator.generateValues();

		// then
		List<Long> expectedBackoffs = Arrays.asList(1000L, 1000L);
		assertEquals(expectedBackoffs, backOffValues);
	}

	@Test
	void shouldGenerateExponentialValues() {

		// setup
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setMultiplier(2);
		backOffPolicy.setInitialInterval(1000);
		BackOffValuesGenerator backOffValuesGenerator = new BackOffValuesGenerator(4, backOffPolicy);

		// when
		List<Long> backOffValues = backOffValuesGenerator.generateValues();

		// then
		List<Long> expectedBackoffs = Arrays.asList(1000L, 2000L, 4000L);
		assertEquals(expectedBackoffs, backOffValues);
	}

	@Test
	void shouldGenerateWithNoBackOff() {

		// setup
		BackOffPolicy backOffPolicy = new NoBackOffPolicy();
		BackOffValuesGenerator backOffValuesGenerator = new BackOffValuesGenerator(4, backOffPolicy);

		// when
		List<Long> backOffValues = backOffValuesGenerator.generateValues();

		// then
		List<Long> expectedBackoffs = Arrays.asList(0L, 0L, 0L);
		assertEquals(expectedBackoffs, backOffValues);
	}
}