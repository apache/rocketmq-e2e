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

package org.apache.rocketmq.utils.data.collect;

import java.util.Collection;
public interface DataCollector<T> {
    /**
     * reset datas' size to zero
     */
    void resetData();

    /**
     * Get all collected data, including duplicate data
     *
     * @return
     */
    Collection<T> getAllData();

    /**
     * Get collected non-duplicate data
     *
     * @return
     */
    Collection<T> getAllDataWithoutDuplicate();

    /**
     * Add data
     */
    void addData(T data);

    /**
     * Gets the total number of non-duplicate data
     *
     * @return
     */
    long getDataSizeWithoutDuplicate();

    /**
     * Gets the total number of all data, including duplicates
     *
     * @return
     */
    long getDataSize();

    /**
     * Verify that a piece of data is duplicated
     *
     * @return
     */
    boolean isRepeatedData(T data);

    /**
     * Gets the number of times a piece of data is repeated
     *
     * @return
     */
    int getRepeatedTimeForData(T data);

    /**
     * Remove a piece of data
     *
     * @return
     */
    void removeData(T data);

    /**
     * Lock the counter so that it no longer increases, but can be deleted
     *
     * @return
     */
    void lockIncrement();

    /**
     * The contact counter locks so that it can be added and removed
     *
     * @return
     */
    void unlockIncrement();


    T getFirstElement();
}
