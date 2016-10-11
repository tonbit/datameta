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

#ifndef STDEX_DATA_SOURCE_MYSQL_H_
#define STDEX_DATA_SOURCE_MYSQL_H_
#ifdef STDEX_HAS_MYSQL

#include "data_meta.h"
#include <mysql.h>
namespace stdex {

class DataSourceMysql
{
public:
	DataSourceMysql();
	~DataSourceMysql();
	bool is_ready() const;

	int open(const string &host, int port, const string &user, const string &passwd, const string &dbase);
	void close();

	int query(const string &sql, std::vector<Meta> &in, std::vector<Meta> &row);
	int query_all(const string &sql, std::vector<Meta> &in, std::vector<std::vector<Meta>> &rows);
	int insert(const string &sql, std::vector<Meta> &in, int64_t *insert_id=NULL);
	int execute(const string &sql, std::vector<Meta> &in, int64_t *affected=NULL);
	int execute(const string &sql);

	unsigned last_errno() const;
	const char *last_error() const;

private:
	MYSQL _dbase;
	bool _ready;
	unsigned _errno;
	string _error;
};

}
#endif
#endif //STDEX_DATA_SOURCE_MYSQL_H_