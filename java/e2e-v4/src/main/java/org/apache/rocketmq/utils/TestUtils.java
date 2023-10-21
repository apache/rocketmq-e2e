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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestUtils {
    private static Logger logger = LoggerFactory.getLogger(TestUtils.class);

    private TestUtils() {
    }

    public static Long parseStringToLong(String s, Long defval) {
        Long val;
        try {
            val = Long.parseLong(s);
        } catch (NumberFormatException var4) {
            val = defval;
        }

        return val;
    }

    public static Integer parseStringToInteger(String s, Integer defval) {
        Integer val;
        try {
            val = Integer.parseInt(s);
        } catch (NumberFormatException var4) {
            val = defval;
        }

        return val;
    }

    public static String addQuoteToParamater(String param) {
        StringBuilder sb = new StringBuilder("'");
        sb.append(param).append("'");
        return sb.toString();
    }

    public static void waitForMoment(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException var3) {
            var3.printStackTrace();
        }

    }

    public static void waitForSeconds(long time) {
        try {
            logger.info("waiting {} seconds...", time);
            TimeUnit.SECONDS.sleep(time);
        } catch (InterruptedException var3) {
            var3.printStackTrace();
        }

    }

    public static void waitForMinutes(long time) {
        try {
            logger.info("waiting {} minutes...", time);
            TimeUnit.MINUTES.sleep(time);
        } catch (InterruptedException var3) {
            var3.printStackTrace();
        }

    }

    public static void waitForInputQuit() {
        waitForInput("quit");
    }

    public static void waitForInput(String keyWord) {
        waitForInput(keyWord, String.format("The thread will wait until you input stop command[%s]:", keyWord));
    }

    public static void waitForInput(String keyWord, String info) {
        try {
            byte[] b = new byte[1024];
            int n = System.in.read(b);

            for (String s = (new String(b, 0, n - 1)).replace("\r", "").replace("\n", ""); !s
                    .equals(keyWord); s = new String(b, 0, n - 1)) {
                n = System.in.read(b);
            }

        } catch (IOException var5) {
            var5.printStackTrace();
        }

    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return ((Comparable) o1.getValue()).compareTo(o2.getValue());
            }
        });
        Map<K, V> result = new LinkedHashMap();
        Iterator var3 = list.iterator();

        while (var3.hasNext()) {
            Map.Entry<K, V> entry = (Map.Entry) var3.next();
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

}
