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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    /**
     * Gets the second-level offset time of the current time
     * offset=10 (10 seconds later)  offset=-10 (10 seconds before)
     * @param offset Offset time
     * @return
     */
    public static long getCurrentTimeOffsetBySecond(int offset) {

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, offset);
        Date beforeD = calendar.getTime();
        long time = 0;
        try {
            time = SDF.parse(SDF.format(beforeD)).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }

    /**
     * Gets the minute-level offset time of the current time
     *
     * @param offset Offset time
     * @return
     */
    public static long getCurrentTimeOffsetByMin(int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, offset);
        Date beforeD = calendar.getTime();
        long time = 0;
        try {
            time = SDF.parse(SDF.format(beforeD)).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }

    /**
     * Gets the hour level offset time of the current time
     *
     * @param offset Offset time
     * @return
     */
    public static long getCurrentTimeOffsetByHour(int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR, offset);
        Date beforeD = calendar.getTime();
        long time = 0;
        try {
            time = SDF.parse(SDF.format(beforeD)).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }

    /**
     * Gets the day offset time of the current time
     *
     * @param offset Offset time
     * @return
     */
    public static long getCurrentTimeOffsetByDay(int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, offset);
        Date beforeD = calendar.getTime();
        long time = 0;
        try {
            time = SDF.parse(SDF.format(beforeD)).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }

    /**
     * Formatting timestamp
     *
     * @param time Offset time
     * @return
     */
    public static String format(long time) {
        return SDF.format(time);
    }
}
