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
#include <algorithm>
#include <random>
#include <string>
#include <sstream>
#include <array>

#include "utils/RandomUtils.h"

std::random_device RandomUtils::rd;
std::mt19937 RandomUtils::rng(RandomUtils::rd());

std::string RandomUtils::getStringByUUID()
{
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<int> dist(0, 15);

  std::string res(36, 0);
  const char *hex = "0123456789abcdef";
  for (int i = 0; i < 36; ++i)
  {
    if (i == 8 || i == 13 || i == 18 || i == 23)
    {
      res[i] = '-';
    }
    else
    {
      res[i] = hex[dist(rng)];
    }
  }
  return res;
}

std::string RandomUtils::getChineseWord(int len)
{
  std::uniform_int_distribution<int> dist(UNICODE_START, UNICODE_END);
  std::stringstream ss;
  for (int i = 0; i < len; ++i)
  {
    ss << getChineseChar(dist(rng));
  }
  return ss.str();
}

std::string RandomUtils::getStringWithNumber(int n)
{
  int arg[] = {'0', '9' + 1};
  return getString(n, arg, sizeof(arg) / sizeof(arg[0]));
}

std::string RandomUtils::getStringWithCharacter(int n)
{
  int arg[] = {'a', 'z' + 1, 'A', 'Z' + 1};
  return getString(n, arg, sizeof(arg) / sizeof(arg[0]));
}

int RandomUtils::getIntegerBetween(int n, int m)
{
  if (m == n)
  {
    return n;
  }
  int res = RandomUtils::getIntegerMoreThanZero();
  return n + res % (m - n);
}

int RandomUtils::getIntegerMoreThanZero()
{
  int res = rd();
  while (res <= 0)
  {
    res = rd();
  }
  return res;
}

std::string RandomUtils::getString(int n, int arg[], int size)
{
  std::stringstream ss;
  for (int i = 0; i < n; i++)
  {
    ss << RandomUtils::getChar(arg, size);
  }
  return ss.str();
}

char RandomUtils::getChar(int arg[], int size)
{
  int c = rd() % (size / 2);
  c = c * 2;
  return static_cast<char>(getIntegerBetween(arg[c], arg[c + 1]));
}

char RandomUtils::getChineseChar(int codePoint)
{
  std::stringstream ss;
  ss << std::hex << codePoint;
  std::string hexStr = ss.str();

  if (hexStr.size() > 4)
  {
    return 0;
  }

  std::array<char, 4> bytes;

  for (size_t i = 0; i < hexStr.size(); ++i)
  {
    bytes[i] = static_cast<char>(hexStr[i]);
  }

  int value = std::stoi(hexStr, nullptr, 16);

  if (value < UNICODE_START || value >= UNICODE_END)
  {
    return 0;
  }

  char utf8Bytes[4];
  if (value <= 0x7F)
  {
    utf8Bytes[0] = static_cast<char>(value);
    utf8Bytes[1] = '\0';
    utf8Bytes[2] = '\0';
    utf8Bytes[3] = '\0';
  }
  else if (value <= 0x7FF)
  {
    utf8Bytes[0] = static_cast<char>((value >> 6) | 0xC0);
    utf8Bytes[1] = static_cast<char>((value & 0x3F) | 0x80);
    utf8Bytes[2] = '\0';
    utf8Bytes[3] = '\0';
  }
  else
  {
    utf8Bytes[0] = static_cast<char>((value >> 12) | 0xE0);
    utf8Bytes[1] = static_cast<char>(((value >> 6) & 0x3F) | 0x80);
    utf8Bytes[2] = static_cast<char>((value & 0x3F) | 0x80);
    utf8Bytes[3] = '\0';
  }

  return utf8Bytes[0];
}

std::string RandomUtils::randomAlphabetic(int length)
{
  std::string str("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
  std::random_device rd;
  std::mt19937 generator(rd());

  std::shuffle(str.begin(), str.end(), generator);

  return str.substr(0, length);
}