/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import org.springframework.util.Assert;

/**
 *
 * Provides a convenient API for creating {@link DeadLetterPublishingRecoverer.HeaderNames}.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 * @see DeadLetterPublishingRecoverer.HeaderNames
 *
 */
public class DeadLetterPublishingHeaderNamesBuilder {

	private final Original original = new Original();
	private final Exception exception = new Exception();

	public static DeadLetterPublishingHeaderNamesBuilder.Original original() {
		return new DeadLetterPublishingHeaderNamesBuilder().original;
	}

	public class Original {

		private String offsetHeader;
		private String timestampHeader;
		private String timestampTypeHeader;
		private String topicHeader;
		private String partitionHeader;

		public DeadLetterPublishingHeaderNamesBuilder.Original offsetHeader(String offsetHeader) {
			this.offsetHeader = offsetHeader;
			return this;
		}

		public DeadLetterPublishingHeaderNamesBuilder.Original timestampHeader(String timestampHeader) {
			this.timestampHeader = timestampHeader;
			return this;
		}

		public DeadLetterPublishingHeaderNamesBuilder.Original timestampTypeHeader(String timestampTypeHeader) {
			this.timestampTypeHeader = timestampTypeHeader;
			return this;
		}

		public DeadLetterPublishingHeaderNamesBuilder.Original topicHeader(String topicHeader) {
			this.topicHeader = topicHeader;
			return this;
		}

		public DeadLetterPublishingHeaderNamesBuilder.Original partitionHeader(String partitionHeader) {
			this.partitionHeader = partitionHeader;
			return this;
		}

		public DeadLetterPublishingHeaderNamesBuilder.Exception exception() {
			return DeadLetterPublishingHeaderNamesBuilder.this.exception;
		}

		private DeadLetterPublishingRecoverer.HeaderNames.Original build() {
			Assert.notNull(this.offsetHeader, "offsetHeader header cannot be null");
			Assert.notNull(this.timestampHeader, "timestampHeader header cannot be null");
			Assert.notNull(this.timestampTypeHeader, "timestampTypeHeader header cannot be null");
			Assert.notNull(this.topicHeader, "topicHeader header cannot be null");
			Assert.notNull(this.partitionHeader, "partitionHeader header cannot be null");
			return new DeadLetterPublishingRecoverer.HeaderNames.Original(this.offsetHeader,
					this.timestampHeader,
					this.timestampTypeHeader,
					this.topicHeader,
					this.partitionHeader);
		}
	}

	public class Exception {
		private String keyExceptionFqcn;
		private String exceptionFqcn;
		private String keyExceptionMessage;
		private String exceptionMessage;
		private String keyExceptionStacktrace;
		private String exceptionStacktrace;

		public DeadLetterPublishingHeaderNamesBuilder.Exception keyExceptionFqcn(String keyExceptionFqcn) {
			this.keyExceptionFqcn = keyExceptionFqcn;
			return this;
		}

		public DeadLetterPublishingHeaderNamesBuilder.Exception exceptionFqcn(String exceptionFqcn) {
			this.exceptionFqcn = exceptionFqcn;
			return this;
		}

		public DeadLetterPublishingHeaderNamesBuilder.Exception keyExceptionMessage(String keyExceptionMessage) {
			this.keyExceptionMessage = keyExceptionMessage;
			return this;
		}

		public DeadLetterPublishingHeaderNamesBuilder.Exception exceptionMessage(String exceptionMessage) {
			this.exceptionMessage = exceptionMessage;
			return this;
		}

		public DeadLetterPublishingHeaderNamesBuilder.Exception keyExceptionStacktrace(String keyExceptionStacktrace) {
			this.keyExceptionStacktrace = keyExceptionStacktrace;
			return this;
		}

		public DeadLetterPublishingHeaderNamesBuilder.Exception exceptionStacktrace(String exceptionStacktrace) {
			this.exceptionStacktrace = exceptionStacktrace;
			return this;
		}

		public DeadLetterPublishingRecoverer.HeaderNames build() {
			Assert.notNull(this.keyExceptionFqcn, "keyExceptionFqcn header cannot be null");
			Assert.notNull(this.exceptionFqcn, "exceptionFqcn header cannot be null");
			Assert.notNull(this.keyExceptionMessage, "keyExceptionMessage header cannot be null");
			Assert.notNull(this.exceptionMessage, "exceptionMessage header cannot be null");
			Assert.notNull(this.keyExceptionStacktrace, "keyExceptionStacktrace header cannot be null");
			Assert.notNull(this.exceptionStacktrace, "exceptionStacktrace header cannot be null");
			return new DeadLetterPublishingRecoverer.HeaderNames(DeadLetterPublishingHeaderNamesBuilder.this.original.build(),
					new DeadLetterPublishingRecoverer.HeaderNames.Exception(this.keyExceptionFqcn,
							this.exceptionFqcn,
							this.keyExceptionMessage,
							this.exceptionMessage,
							this.keyExceptionStacktrace,
							this.exceptionStacktrace));
		}
	}
}
