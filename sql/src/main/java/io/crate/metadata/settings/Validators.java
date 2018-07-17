/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.settings;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.set.Sets;

import java.util.Map;
import java.util.Set;

public class Validators {

    private static final String UNSUPPORTED_MESSAGE = "Unsupported setting value: ";
    private static final String INVALID_MESSAGE = "Invalid value for argument '";

    public static Setting.Validator<String> stringValidator(String key, String... allowedValues) {
        if (allowedValues.length == 0) {
            return new StringValidator(key);
        }
        return new StringValidatorAllowedValuesOnly(key, Sets.newHashSet(allowedValues));
    }

    private static class StringValidator implements Setting.Validator<String> {

        private final String key;

        StringValidator(String key) {
            this.key = key;
        }

        @Override
        public void validate(String value, Map settings) {
            if (Booleans.isBoolean(value)) {
                throw new IllegalArgumentException(INVALID_MESSAGE + key + "'");
            }
            try {
                Long.parseLong(value);
                throw new IllegalArgumentException(INVALID_MESSAGE + key + "'");
            } catch (NumberFormatException e) {
                // pass, not a number
            }
        }
    }

    private static class StringValidatorAllowedValuesOnly extends StringValidator {

        private final Set<String> allowedValues;

        StringValidatorAllowedValuesOnly(String key, Set<String> allowedValues) {
            super(key);
            this.allowedValues = allowedValues;
        }

        @Override
        public void validate(String value, Map settings) {
            super.validate(value, settings);
            if (value.isEmpty() == false && allowedValues.contains(value) == false) {
                throw new IllegalArgumentException(UNSUPPORTED_MESSAGE + value);
            }
        }
    }
}
