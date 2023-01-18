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
     * 获取当前时间的秒级偏移时间
     * offset=10 10秒后  offset=-10 10秒前
     * @param offset 偏移时间
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
     * 获取当前时间的分钟级偏移时间
     *
     * @param offset 偏移时间
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
     * 获取当前时间的小时级偏移时间
     *
     * @param offset 偏移时间
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
     * 获取当前时间的天级偏移时间
     *
     * @param offset 偏移时间
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
     * 格式化时间戳
     *
     * @param time 偏移时间
     * @return
     */
    public static String format(long time) {
        return SDF.format(time);
    }
}
