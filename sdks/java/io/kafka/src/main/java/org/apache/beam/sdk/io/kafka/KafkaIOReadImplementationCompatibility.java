/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.sdk.io.kafka.KafkaIOReadImplementationCompatibility.KafkaIOReadImplementationCompatibilityType.BOTH;
import static org.apache.beam.sdk.io.kafka.KafkaIOReadImplementationCompatibility.KafkaIOReadImplementationCompatibilityType.ONLY_LEGACY;
import static org.apache.beam.sdk.io.kafka.KafkaIOReadImplementationCompatibility.KafkaIOReadImplementationCompatibilityType.ONLY_SDF;

import java.lang.reflect.Method;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;

class KafkaIOReadImplementationCompatibility {

  enum KafkaIOReadImplementationCompatibilityType {
    BOTH,
    ONLY_LEGACY,
    ONLY_SDF
  }

  @VisibleForTesting
  @SuppressWarnings("ImmutableEnumChecker")
  enum KafkaIOReadProperties {
    CONSUMER_CONFIG(BOTH),
    TOPICS(BOTH),
    TOPIC_PARTITIONS(BOTH),
    KEY_CODER(BOTH),
    VALUE_CODER(BOTH),
    CONSUMER_FACTORY_FN(BOTH),
    WATERMARK_FN(ONLY_LEGACY),
    MAX_NUM_RECORDS(ONLY_LEGACY, Long.MAX_VALUE),
    MAX_READ_TIME(ONLY_LEGACY),
    START_READ_TIME(BOTH),
    STOP_READ_TIME(ONLY_SDF),
    COMMIT_OFFSETS_IN_FINALIZE_ENABLED(BOTH, false),
    DYNAMIC_READ(ONLY_SDF, false),
    WATCH_TOPIC_PARTITION_DURATION(ONLY_SDF),
    TIMESTAMP_POLICY_FACTORY(BOTH),
    OFFSET_CONSUMER_CONFIG(BOTH),
    KEY_DESERIALIZER_PROVIDER(BOTH),
    VALUE_DESERIALIZER_PROVIDER(BOTH),
    CHECK_STOP_READING_FN(ONLY_SDF),
    ;

    @Nonnull
    private final KafkaIOReadImplementationCompatibilityType implementationCompatibilityType;

    private final Object defaultValue;

    private final Method getterMethod;

    private KafkaIOReadProperties(
        @Nonnull KafkaIOReadImplementationCompatibilityType implementationCompatibilityType) {
      this(implementationCompatibilityType, null);
    }

    private KafkaIOReadProperties(
        @Nonnull KafkaIOReadImplementationCompatibilityType implementationCompatibilityType,
        @Nullable Object defaultValue) {
      this.implementationCompatibilityType =
          Preconditions.checkNotNull(implementationCompatibilityType);
      this.defaultValue = defaultValue;
      final String propertyNameInUpperCamel =
          CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name());
      Method getterMethod;
      try {
        getterMethod = KafkaIO.Read.class.getDeclaredMethod("get" + propertyNameInUpperCamel);
      } catch (NoSuchMethodException e) {
        try {
          getterMethod = KafkaIO.Read.class.getDeclaredMethod("is" + propertyNameInUpperCamel);
        } catch (NoSuchMethodException e2) {
          throw new RuntimeException("Should not happen", e);
        }
      }
      this.getterMethod = getterMethod;
    }

    @VisibleForTesting
    Object getDefaultValue() {
      return defaultValue;
    }

    @VisibleForTesting
    Method getGetterMethod() {
      return getterMethod;
    }
  }

  static KafkaIOReadImplementationCompatibilityType getAllowedType(KafkaIO.Read<?, ?> read) {
    final Multimap<KafkaIOReadImplementationCompatibilityType, KafkaIOReadProperties>
        requiredTypesWithAssociatedProperties = HashMultimap.create();
    for (KafkaIOReadProperties property : KafkaIOReadProperties.values()) {
      if (property.implementationCompatibilityType == BOTH) {
        // if both type is allowed don't even run this check,
        // as the requiredType would be nothing
        continue;
      }
      final Object defaultValue = property.defaultValue;
      final Object currentValue;
      try {
        currentValue = property.getterMethod.invoke(read);
      } catch (Exception e) {
        throw new RuntimeException("Should not happen", e);
      }
      if (Objects.equals(defaultValue, currentValue)) {
        // the defaultValue is always allowed,
        // so the requiredType would be nothing
        continue;
      }
      // the property has got a value, so we require the corresponding type
      requiredTypesWithAssociatedProperties.put(property.implementationCompatibilityType, property);
    }
    switch (requiredTypesWithAssociatedProperties.keySet().size()) {
      case 0:
        // there is nothing required, so we accept both
        return BOTH;
      case 1:
        // either the legacy or the sdf
        return Iterables.getOnlyElement(requiredTypesWithAssociatedProperties.keySet());
      case 2:
      default:
        throw new IllegalStateException(
            "The Kafka read has been configured in a way that it requires both legacy and SDF-based read implementation at once! "
                + "Properties that require legacy: "
                + requiredTypesWithAssociatedProperties.get(ONLY_LEGACY)
                + ", "
                + "Properties that require SDF: "
                + requiredTypesWithAssociatedProperties.get(ONLY_SDF));
    }
  }
}
