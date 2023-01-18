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
     * 计数归零
     */
    void resetData();

    /**
     * 取得所有的收集数据，包含重复的数据
     *
     * @return
     */
    Collection<T> getAllData();

    /**
     * 取得收集的非重复数据
     *
     * @return
     */
    Collection<T> getAllDataWithoutDuplicate();

    /**
     * 增加数据
     */
    void addData(T data);

    /**
     * 取得非重复数据的总个数
     *
     * @return
     */
    long getDataSizeWithoutDuplicate();

    /**
     * 取得所有数据的总个数，包括重复数据
     *
     * @return
     */
    long getDataSize();

    /**
     * 验证一条数据是否存在重复
     *
     * @return
     */
    boolean isRepeatedData(T data);

    /**
     * 取得一条数据重复出现的次数
     *
     * @return
     */
    int getRepeatedTimeForData(T data);

    /**
     * 移除一条数据
     *
     * @return
     */
    void removeData(T data);

    /**
     * 锁定计数器，使其不再增加，但是可以删除
     *
     * @return
     */
    void lockIncrement();

    /**
     * 接触计数器锁定，使其可以增加和删除
     *
     * @return
     */
    void unlockIncrement();


    T getFirstElement();
}
