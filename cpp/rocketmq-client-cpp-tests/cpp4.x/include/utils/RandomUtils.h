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
#pragma once

#include <random>
#include <string>
#include <sstream>

class RandomUtils
{

public:
    RandomUtils() = delete;

    static std::string getStringByUUID();
    static std::string getChineseWord(int len);
    static std::string getStringWithNumber(int n);
    static std::string getStringWithCharacter(int n);
    static int getIntegerBetween(int n, int m);
    static int getIntegerMoreThanZero();
    static std::string randomAlphabetic(int length);

private:
    static constexpr int UNICODE_START = 0x4E00;
    static constexpr int UNICODE_END = 0x9FA0;
    static std::random_device rd;
    static std::mt19937 rng;

    static std::string getString(int n, int arg[], int size);
    static char getChar(int arg[], int size);
    static char getChineseChar(int codePoint);
};