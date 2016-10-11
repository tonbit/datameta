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

#ifdef STDEX_HAS_SQLITE
#include "data_source_sqlite.h"
namespace stdex {

DataSourceSqlite::DataSourceSqlite()
{
	int ret = sqlite3_config(SQLITE_CONFIG_SERIALIZED);
	assert(ret == SQLITE_OK);

	db = NULL;
}

DataSourceSqlite::~DataSourceSqlite()
{
	close();
}

int DataSourceSqlite::open(const string &filename)
{
	assert(db == NULL);

	if (sqlite3_open_v2(filename.c_str(), &db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL) != SQLITE_OK)
		return 1;

	//autocommit mode is on by default
	return 0;
}

void DataSourceSqlite::close()
{
	if (db)
	{
		sqlite3_close_v2(db);
		db = NULL;
	}
}

bool DataSourceSqlite::is_ready()
{
	if (db)
		return true;
	else
		return false;
}

int DataSourceSqlite::query(const string &sql, std::vector<Meta> &in, std::vector<Meta> &row)
{
	sqlite3_stmt *stmt = NULL;

	if (sqlite3_prepare_v2(db, sql.c_str(), sql.size(), &stmt, NULL) != SQLITE_OK)
		return 1;

	if (!in.empty())
	{
		for (size_t i=0; i<in.size(); i++)
		{
			const Meta &meta = in[i];

			if (meta.is_integer())
			{
				i32 val = meta.get_integer();
				if (sqlite3_bind_int(stmt, i+1, val))
					return 2;
			}
			else if (meta.is_bigint())
			{
				i64 val = meta.get_bigint();
				if (sqlite3_bind_int64(stmt, i+1, val))
					return 2;
			}
			else if (meta.is_float())
			{
				f32 val = meta.get_float();
				if (sqlite3_bind_double(stmt, i+1, val))
					return 2;
			}
			else if (meta.is_double())
			{
				f64 val = meta.get_double();
				if (sqlite3_bind_double(stmt, i+1, val))
					return 2;
			}
			else if (meta.is_string())
			{
				string val = meta.get_string();
				if (sqlite3_bind_text(stmt, i+1, val.c_str(), val.size(), SQLITE_TRANSIENT))
					return 2;
			}
			else
			{
				if (sqlite3_bind_null(stmt, i+1))
					return 2;
			}
		}
	}

	int ret = sqlite3_step(stmt);

	if (ret == SQLITE_DONE)
	{
        sqlite3_finalize(stmt);
        return 0;
	}

	if (ret != SQLITE_ROW)
	{
		printf("\nsqlite err: %s: %s\n", sql.c_str(), sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return 3;
	}

	int col_count = sqlite3_column_count(stmt);
	row.resize(col_count);

	for (int i=0; i<col_count; i++)
	{
		sqlite3_value *col_value = sqlite3_column_value(stmt, i);
		const char *col_type = sqlite3_column_decltype(stmt, i);
		int value_type = sqlite3_value_type(col_value);
		if (value_type == SQLITE_INTEGER && col_type != "BIGINT")
		{
			row[i] = (i32)sqlite3_value_int(col_value);
		}
		else if (value_type == SQLITE_INTEGER && col_type == "BIGINT")
		{
			row[i] = (i64)sqlite3_value_int64(col_value);
		}
		else if (value_type == SQLITE_FLOAT)
		{
			row[i] = (f64)sqlite3_value_double(col_value);
		}
		else if (value_type == SQLITE3_TEXT)
		{
			string tmp((const char *)sqlite3_value_text(col_value), sqlite3_value_bytes(col_value));
			row[i] = std::move(tmp);
		}
		else if (value_type == SQLITE_BLOB)
		{
			string tmp((const char *)sqlite3_value_blob(col_value), sqlite3_value_bytes(col_value));
			row[i] = std::move(tmp);
		}
		else if (value_type == SQLITE_NULL)
		{
			row[i] = std::move(string());
		}
	}

    sqlite3_finalize(stmt);
	return 0;
}

int DataSourceSqlite::query_all(const string &sql, std::vector<Meta> &in, std::vector<std::vector<Meta>> &rows)
{
	sqlite3_stmt *stmt = NULL;

	if (sqlite3_prepare_v2(db, sql.c_str(), sql.size(), &stmt, NULL) != SQLITE_OK)
		return 1;

	if (!in.empty())
	{
		for (size_t i=0; i<in.size(); i++)
		{
			const Meta &meta = in[i];

			if (meta.is_integer())
			{
				i32 val = meta.get_integer();
				sqlite3_bind_int(stmt, i+1, val);
			}
			else if (meta.is_bigint())
			{
				i64 val = meta.get_bigint();
				sqlite3_bind_int64(stmt, i+1, val);
			}
			else if (meta.is_float())
			{
				f32 val = meta.get_float();
				sqlite3_bind_double(stmt, i+1, val);
			}
			else if (meta.is_double())
			{
				f64 val = meta.get_double();
				sqlite3_bind_double(stmt, i+1, val);
			}
			else if (meta.is_string())
			{
				string val = meta.get_string();
				if (sqlite3_bind_text(stmt, i+1, val.c_str(), val.size(), SQLITE_TRANSIENT))
					return 2;
			}
			else
			{
				sqlite3_bind_null(stmt, i+1);
			}
		}
	}

	while (true)
	{
		int ret = sqlite3_step(stmt);

		if (ret == SQLITE_DONE)
		{
			sqlite3_finalize(stmt);
			return 0;
		}

		if (ret != SQLITE_ROW)
		{
			printf("\nsqlite err: %s: %s\n", sql.c_str(), sqlite3_errmsg(db));
			sqlite3_finalize(stmt);
			return 3;
		}

		int col_count = sqlite3_column_count(stmt);
		std::vector<Meta> row;
		row.resize(col_count);

		for (int i=0; i<col_count; i++)
		{
			sqlite3_value *col_value = sqlite3_column_value(stmt, i);
			const char *col_type = sqlite3_column_decltype(stmt, i);
			int value_type = sqlite3_value_type(col_value);
			if (value_type == SQLITE_INTEGER && col_type != "BIGINT")
			{
				row[i] = (i32)sqlite3_value_int(col_value);
			}
			else if (value_type == SQLITE_INTEGER && col_type == "BIGINT")
			{
				row[i] = (i64)sqlite3_value_int(col_value);
			}
			else if (value_type == SQLITE_FLOAT)
			{
				row[i] = (f64)sqlite3_value_double(col_value);
			}
			else if (value_type == SQLITE3_TEXT)
			{
				string tmp((const char *)sqlite3_value_text(col_value), sqlite3_value_bytes(col_value));
				row[i] = std::move(tmp);
			}
			else if (value_type == SQLITE_BLOB)
			{
				string tmp((const char *)sqlite3_value_blob(col_value), sqlite3_value_bytes(col_value));
				row[i] = std::move(tmp);
			}
			else if (value_type == SQLITE_NULL)
			{
				row[i] = std::move(string());
			}
		}

		rows.push_back(std::move(row));
	}

	sqlite3_finalize(stmt);
	return 0;
}


int DataSourceSqlite::insert(const string &sql, std::vector<Meta> &in, i64 *insert_id)
{
	sqlite3_stmt *stmt = NULL;

	if (sqlite3_prepare_v2(db, sql.c_str(), sql.size(), &stmt, NULL) != SQLITE_OK)
		return 1;

	if (!in.empty())
	{
		for (size_t i=0; i<in.size(); i++)
		{
			const Meta &meta = in[i];

			if (meta.is_integer())
			{
				i32 val = meta.get_integer();
				sqlite3_bind_int(stmt, i+1, val);
			}
			else if (meta.is_bigint())
			{
				i64 val = meta.get_bigint();
				sqlite3_bind_int64(stmt, i+1, val);
			}
			else if (meta.is_float())
			{
				f32 val = meta.get_float();
				sqlite3_bind_double(stmt, i+1, val);
			}
			else if (meta.is_double())
			{
				f64 val = meta.get_double();
				sqlite3_bind_double(stmt, i+1, val);
			}
			else if (meta.is_string())
			{
				string &val = meta.get_string();
				if (sqlite3_bind_text(stmt, i+1, val.c_str(), val.size(), SQLITE_TRANSIENT))
					return 2;
			}
			else
			{
				sqlite3_bind_null(stmt, i+1);
			}
		}
	}

	int ret = sqlite3_step(stmt);

	if (ret != SQLITE_DONE)
	{
		printf("\nsqlite err: %s: %s\n", sql.c_str(), sqlite3_errmsg(db));
		sqlite3_finalize(stmt);
		return 3;
	}

	if (insert_id)
		*insert_id = sqlite3_last_insert_rowid(db);

	sqlite3_finalize(stmt);
	return 0;
}


int DataSourceSqlite::execute(const string &sql, std::vector<Meta> &in, i64 *affected)
{
	sqlite3_stmt *stmt = NULL;

	if (sqlite3_prepare_v2(db, sql.c_str(), sql.size(), &stmt, NULL) != SQLITE_OK)
		return 1;

	if (!in.empty())
	{
		for (size_t i=0; i<in.size(); i++)
		{
			const Meta &meta = in[i];

			if (meta.is_integer())
			{
				i32 val = meta.get_integer();
				sqlite3_bind_int(stmt, i+1, val);
			}
			else if (meta.is_bigint())
			{
				i64 val = meta.get_bigint();
				sqlite3_bind_int64(stmt, i+1, val);
			}
			else if (meta.is_float())
			{
				f32 val = meta.get_float();
				sqlite3_bind_double(stmt, i+1, val);
			}
			else if (meta.is_double())
			{
				f64 val = meta.get_double();
				sqlite3_bind_double(stmt, i+1, val);
			}
			else if (meta.is_string())
			{
				string val = meta.get_string();
				if (sqlite3_bind_text(stmt, i+1, val.c_str(), val.size(), SQLITE_TRANSIENT))
					return 2;
			}
			else
			{
				sqlite3_bind_null(stmt, i+1);
			}
		}
	}

	int ret = sqlite3_step(stmt);

	if (ret != SQLITE_DONE)
	{
		printf("\nsqlite err: %s: %s\n", sql.c_str(), sqlite3_errmsg(db));
		sqlite3_finalize(stmt);
		return 3;
	}

	if (affected)
		*affected = sqlite3_changes(db);

	sqlite3_finalize(stmt);
	return 0;
}

int DataSourceSqlite::execute(const string &sql)
{
	if (sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL) != SQLITE_OK)
	{
		printf("\nsqlite err: %s: %s\n", sql.c_str(), sqlite3_errmsg(db));
		return 1;
	}

	return 0;
}

u32 DataSourceSqlite::last_errno()
{
	return sqlite3_errcode(db);
}

const char *DataSourceSqlite::last_error()
{
	return sqlite3_errmsg(db);
}

}
#endif