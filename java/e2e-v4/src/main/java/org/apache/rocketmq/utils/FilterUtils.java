/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class FilterUtils {
    private static final Logger log = LoggerFactory.getLogger(AssertUtils.class);

    // Custom used to filter tag for pullconsumer
    public static boolean inTags(String msgTag, String tags) {
        if (null == tags || tags.equals("*") || tags.length() == 0) {
            return true;
        } else {
            // partition
            String[] tag_splits = tags.split("\\|\\|");
            Set<Integer> tagSet = new HashSet<>();
            if (tag_splits.length > 0) {
                for (String tag : tag_splits) {
                    if (tag.length() > 0) {
                        if ("*".equals(tag))
                            return true;
                        String trimString = tag.trim();
                        if (trimString.length() > 0) {
                            tagSet.add(trimString.hashCode());
                        }
                    }
                    if (tagSet.contains(msgTag.hashCode())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
