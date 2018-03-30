/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef STDEX_DATA_META_H_
#define STDEX_DATA_META_H_

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <string>
#include <array>
#include <vector>
#include <list>
#include <map>
#include <unordered_map>
#include <set>
#include <unordered_set>

#ifdef _MSC_VER
#include <winsock2.h>
#endif

using std::string;
using std::wstring;

typedef unsigned long ulong;
typedef unsigned short ushort;

namespace stdex {

class Meta
{
public:
	Meta()
	{
		type = TYPE_NULL;
	}

    inline Meta(const Meta &other)
    {
        type = other.type;

        if (type == TYPE_STRING)
            _string = other._string;
        else
            _number = other._number;
    }

    inline Meta(Meta &&other)
    {
        type = other.type;

        if (type == TYPE_STRING)
            _string = std::move(other._string);
        else
            _number = other._number;
    }

    inline Meta(int32_t val)
    {
        type = TYPE_INT;
        _number.number_i32 = val;
    }

    inline Meta(uint32_t val)
    {
        type = TYPE_INT;
        _number.number_i32 = (int32_t)val;
    }

#ifndef _MSC_VER
    inline Meta(size_t val)
    {
        type = TYPE_INT;
        _number.number_i32 = (int32_t)val;
    }
#endif
	
    inline Meta(int64_t val)
    {
        type = TYPE_BIGINT;
        _number.number_i64 = val;
    }

    inline Meta(float val)
    {
        type = TYPE_FLOAT;
        _number.number_f32 = val;
    }

    inline Meta(double val)
    {
        type = TYPE_DOUBLE;
        _number.number_f64 = val;
    }

    inline Meta(const string &val)
    {
        type = TYPE_STRING;
        _string = val;
    }

    inline Meta(string &&val)
    {
        type = TYPE_STRING;
        _string = std::move(val);
    }

    inline Meta(const char *val)
    {
        type = TYPE_STRING;
        _string = val;
    }

	inline Meta &operator=(const Meta &other)
    {
        type = other.type;

        if (type == TYPE_STRING)
            _string = other._string;
        else
            _number = other._number;

        return *this;
    }

    inline Meta &operator=(Meta &&other)
    {
        type = other.type;

        if (type == TYPE_STRING)
            _string = std::move(other._string);
        else
            _number = other._number;

        return *this;
    }

    inline Meta &operator=(int32_t val)
    {
        type = TYPE_INT;
        _number.number_i32 = val;
        return *this;
    }

    inline Meta &operator=(uint32_t val)
    {
        type = TYPE_INT;
        _number.number_i32 = (int32_t)val;
        return *this;
    }

    inline Meta &operator=(int64_t val)
    {
        type = TYPE_BIGINT;
        _number.number_i64 = val;
        return *this;
    }

    inline Meta &operator=(float val)
    {
        type = TYPE_FLOAT;
        _number.number_f32 = val;
        return *this;
    }

    inline Meta &operator=(double val)
    {
        type = TYPE_DOUBLE;
        _number.number_f64 = val;
        return *this;
    }

    inline Meta &operator=(const string &val)
    {
        type = TYPE_STRING;
        _string = val;
        return *this;
    }

    inline Meta &operator=(string &&val)
    {
        type = TYPE_STRING;
        _string = std::move(val);
        return *this;
    }

    inline Meta &operator=(const char *val)
    {
        type = TYPE_STRING;
        _string = val;
        return *this;
    }

    inline bool is_null() const
    {
    	return type == TYPE_NULL;
    }

    inline bool is_integer() const
    {
    	return type == TYPE_INT;
    }

    inline bool is_bigint() const
    {
    	return type == TYPE_BIGINT;
    }

    inline bool is_float() const
    {
    	return type == TYPE_FLOAT;
    }

    inline bool is_double() const
    {
    	return type == TYPE_DOUBLE;
    }

    inline bool is_string() const
    {
    	return type == TYPE_STRING;
    }

    inline int32_t &int_ref()
    {
        return _number.number_i32;
    }

    inline int64_t &bigint_ref()
    {
        return _number.number_i64;
    }

    inline float &float_ref()
    {
        return _number.number_f32;
    }

    inline double &double_ref()
    {
        return _number.number_f64;
    }

    inline string &string_ref()
    {
        return _string;
    }

    inline int32_t get_int() const
    {
        return _number.number_i32;
    }

    inline int64_t get_bigint() const
    {
        return _number.number_i64;
    }

    inline float get_float() const
    {
        return _number.number_f32;
    }

    inline double get_double() const
    {
        return _number.number_f64;
    }

    inline string get_string() const
    {
        return _string;
    }

    inline string&& move_string()
    {
        return std::move(_string);
    }

    inline string to_string() const
    {
    	if (type == TYPE_INT)
    	{
    		return std::to_string(_number.number_i32);
    	}
    	else if (type == TYPE_BIGINT)
    	{
    		return std::to_string(_number.number_i64);
    	}
    	else if (type == TYPE_FLOAT)
    	{
    		return std::to_string(_number.number_f32);
    	}
    	else if (type == TYPE_DOUBLE)
    	{
    		return std::to_string(_number.number_f64);
    	}
    	else if (type == TYPE_STRING)
    	{
    		return _string;
    	}

    	return string();
    }

private:
	enum Type
	{
		TYPE_NULL,
		TYPE_INT,
		TYPE_BIGINT,
		TYPE_FLOAT,
		TYPE_DOUBLE,
		TYPE_STRING,
	};

    Type type;

    union Number
    {
    	int32_t number_i32;
    	int64_t number_i64;
    	float number_f32;
    	double number_f64;
    };

    Number _number;
    string _string;
};

}
#endif //STDEX_DATA_META_H_