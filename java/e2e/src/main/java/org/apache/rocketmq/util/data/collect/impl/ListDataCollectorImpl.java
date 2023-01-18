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

package org.apache.rocketmq.util.data.collect.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.rocketmq.util.data.collect.DataCollector;

public class ListDataCollectorImpl<T> implements DataCollector<T> {

    private final List<T> datas = new ArrayList<>();
    private boolean lock = false;

    public ListDataCollectorImpl() {

    }

    public ListDataCollectorImpl(Collection<T> datas) {
        for (T data : datas) {
            addData(data);
        }
    }

    public Collection<T> getAllData() {
        return datas;
    }

    public synchronized void resetData() {
        datas.clear();
        unlockIncrement();
    }

    public long getDataSizeWithoutDuplicate() {
        return getAllDataWithoutDuplicate().size();
    }

    public synchronized void addData(T data) {
        if (lock) {
            return;
        }
        datas.add(data);
    }

    public long getDataSize() {
        return datas.size();
    }

    public boolean isRepeatedData(Object data) {
        return Collections.frequency(datas, data) == 1;
    }

    public synchronized Collection<T> getAllDataWithoutDuplicate() {
        return new HashSet<T>(datas);
    }

    public int getRepeatedTimeForData(Object data) {
        int res = 0;
        for (Object obj : datas) {
            if (obj.equals(data)) {
                res++;
            }
        }
        return res;
    }

    public synchronized void removeData(Object data) {
        datas.remove(data);
    }

    public void lockIncrement() {
        lock = true;
    }

    public void unlockIncrement() {
        lock = false;
    }

    @Override
    public T getFirstElement() {
        return datas.get(0);
    }

//    @Override public DataCollector getCopyData() {
//        return new ArrayList<T>(datas);
//    }
}
