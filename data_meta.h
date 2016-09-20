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

#ifndef CCLIENT_DATA_META_H_
#define CCLIENT_DATA_META_H_

#include <limits.h>
#include <stdint.h>
#include <float.h>
#include <assert.h>
#include <errno.h>
#include <stddef.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <ctype.h>
#include <wctype.h>
#include <locale.h>
#include <wchar.h>

#ifdef _MSC_VER

#include <sys/types.h>
#include <winsock2.h>
#include <Ws2ipdef.h>
#include <Ws2tcpip.h>

#else //_MSC_VER

#include <unistd.h>
#include <net/if.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>

#endif

typedef uint64_t u64;
typedef uint32_t u32;
typedef uint16_t u16;
typedef uint8_t  u8;

typedef int64_t  i64;
typedef int32_t  i32;
typedef int16_t  i16;

typedef float    f32;
typedef double   f64;

typedef unsigned long int ulong;
typedef unsigned short int ushort;

#include <string>
using std::string;
using std::wstring;

#include <array>
#include <vector>
#include <list>
#include <map>
#include <unordered_map>
#include <set>
#include <unordered_set>

namespace cclient {

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

    inline Meta(i32 val)
    {
        type = TYPE_INTEGER;
        _number.number_i32 = val;
    }

    inline Meta(u32 val)
    {
        type = TYPE_INTEGER;
        _number.number_i32 = (i32)val;
    }

    inline Meta(i64 val)
    {
        type = TYPE_BIGINT;
        _number.number_i64 = val;
    }

    inline Meta(f32 val)
    {
        type = TYPE_FLOAT;
        _number.number_f32 = val;
    }

    inline Meta(f64 val)
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

    inline Meta &operator=(i32 val)
    {
        type = TYPE_INTEGER;
        _number.number_i32 = val;
        return *this;
    }

    inline Meta &operator=(u32 val)
    {
        type = TYPE_INTEGER;
        _number.number_i32 = (i32)val;
        return *this;
    }

    inline Meta &operator=(i64 val)
    {
        type = TYPE_BIGINT;
        _number.number_i64 = val;
        return *this;
    }

    inline Meta &operator=(f32 val)
    {
        type = TYPE_FLOAT;
        _number.number_f32 = val;
        return *this;
    }

    inline Meta &operator=(f64 val)
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
    	return type == TYPE_INTEGER;
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

    inline i32 &integer_ref()
    {
        return _number.number_i32;
    }

    inline i64 &bigint_ref()
    {
        return _number.number_i64;
    }

    inline f32 &float_ref()
    {
        return _number.number_f32;
    }

    inline f64 &double_ref()
    {
        return _number.number_f64;
    }

    inline string &string_ref()
    {
        return _string;
    }

    inline i32 get_integer() const
    {
        return _number.number_i32;
    }

    inline i64 get_bigint() const
    {
        return _number.number_i64;
    }

    inline f32 get_float() const
    {
        return _number.number_f32;
    }

    inline f64 get_double() const
    {
        return _number.number_f64;
    }

    inline string get_string() const
    {
        return _string;
    }

    inline string to_string() const
    {
    	if (type == TYPE_INTEGER)
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
		TYPE_INTEGER,
		TYPE_BIGINT,
		TYPE_FLOAT,
		TYPE_DOUBLE,
		TYPE_STRING,
	};

    Type type;

    union Number
    {
    	i32 number_i32;
    	i64 number_i64;
    	f32 number_f32;
    	f64 number_f64;
    };

    Number _number;
    string _string;
};

}
#endif //CCLIENT_DATA_META_H_
