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

#include "data_source_mysql.h"
namespace cclient {

	DataSourceMysql::DataSourceMysql()
	{
		mysql_init(&_dbase);
		_ready = false;
	}

	DataSourceMysql::~DataSourceMysql()
	{
		close();
	}

	bool DataSourceMysql::is_ready()
	{
		return _ready;
	}

	int DataSourceMysql::open(const string &host, int port, const string &user, const string &passwd, const string &dbase)
	{
		if (!mysql_real_connect(&_dbase, host.c_str(), user.c_str(), passwd.c_str(), dbase.c_str(), port, NULL, 0))
			return 1;

		mysql_autocommit(&_dbase, 1);
		_ready = true;
		return 0;
	}

	void DataSourceMysql::close()
	{
		if (_ready)
		{
			mysql_close(&_dbase);
			_ready = false;
		}
	}

	int DataSourceMysql::query(const string &sql, std::vector<Meta> &in, std::vector<Meta> &row)
	{
		MYSQL_STMT *stmt = mysql_stmt_init(&_dbase);
		if (!stmt)
			return 1;

		if (mysql_stmt_prepare(stmt, sql.c_str(), sql.size()))
		{
			mysql_stmt_close(stmt);
			return 2;
		}

		std::vector<MYSQL_BIND> param_binds;
		std::vector<ulong> param_lens;

		if (!in.empty())
		{
			param_binds.resize(in.size());
			param_lens.resize(in.size());

			for (size_t i = 0; i < in.size(); i++)
			{
				MYSQL_BIND &bind = param_binds[i];
				memset(&bind, 0, sizeof(bind));
				ulong &len = param_lens[i];
				Meta &meta = in[i];

				if (meta.is_integer())
				{
					i32 &val = meta.integer_ref();

					bind.buffer_type = MYSQL_TYPE_LONG;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_bigint())
				{
					i64 &val = meta.bigint_ref();

					bind.buffer_type = MYSQL_TYPE_LONGLONG;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_float())
				{
					f32 &val = meta.float_ref();

					bind.buffer_type = MYSQL_TYPE_FLOAT;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_double())
				{
					f64 &val = meta.double_ref();

					bind.buffer_type = MYSQL_TYPE_DOUBLE;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_string())
				{
					string &val = meta.string_ref();
					len = val.size();

					bind.buffer_type = MYSQL_TYPE_STRING;
					bind.buffer = (char *)&val[0];
					bind.buffer_length = len;
					bind.length = &len;
				}
				else
				{
					bind.buffer_type = MYSQL_TYPE_NULL;
					bind.buffer = NULL;
				}
			}

			if (mysql_stmt_bind_param(stmt, &param_binds[0]))
			{
				mysql_stmt_close(stmt);
				return 3;
			}
		}

		if (mysql_stmt_execute(stmt))
		{
			mysql_stmt_close(stmt);
			return 4;
		}

		MYSQL_RES *result_meta = mysql_stmt_result_metadata(stmt);
		if (!result_meta)
		{
			mysql_stmt_close(stmt);
			return 5;
		}

		MYSQL_FIELD *fields = mysql_fetch_fields(result_meta);
		u32 field_num = mysql_num_fields(result_meta);
		mysql_free_result(result_meta);

		std::vector<MYSQL_BIND> result_binds;
		std::vector<my_bool> isnull_vec;
		std::vector<ulong> length_vec;

		row.resize(field_num);
		result_binds.resize(field_num);
		isnull_vec.resize(field_num);
		length_vec.resize(field_num);

		for (u32 i = 0; i < field_num; i++)
		{
			MYSQL_BIND &bind = result_binds[i];
			memset(&bind, 0, sizeof(bind));

			MYSQL_FIELD &field = fields[i];
			Meta &meta = row[i];
			my_bool &isnull = isnull_vec[i];
			ulong &length = length_vec[i];

			if (field.type == MYSQL_TYPE_TINY || field.type == MYSQL_TYPE_SHORT || field.type == MYSQL_TYPE_LONG)
			{
				meta = (i32)0;
				i32 &val = meta.integer_ref();

				bind.buffer_type = MYSQL_TYPE_LONG;
				bind.buffer = (char *)&val;
				bind.is_null = &isnull;
			}
			else if (field.type == MYSQL_TYPE_LONGLONG)
			{
				meta = (i64)0;
				i64 &val = meta.bigint_ref();

				bind.buffer_type = MYSQL_TYPE_LONGLONG;
				bind.buffer = (char *)&val;
				bind.is_null = &isnull;
			}
			else if (field.type == MYSQL_TYPE_FLOAT)
			{
				meta = (f32)0;
				f32 &val = meta.float_ref();

				bind.buffer_type = MYSQL_TYPE_FLOAT;
				bind.buffer = (char *)&val;
				bind.is_null = &isnull;
			}
			else if (field.type == MYSQL_TYPE_DOUBLE)
			{
				meta = (f64)0;
				f64 &val = meta.double_ref();

				bind.buffer_type = MYSQL_TYPE_DOUBLE;
				bind.buffer = (char *)&val;
				bind.is_null = &isnull;
			}
			else if (field.type == MYSQL_TYPE_STRING || field.type == MYSQL_TYPE_VAR_STRING)
			{
				meta = "";
				string &val = meta.string_ref();
				val.resize(field.length);

				bind.buffer_type = field.type;
				bind.buffer = (char *)&val[0];
				bind.buffer_length = val.size();
				bind.length = &length;
				bind.is_null = &isnull;
			}
		}

		if (mysql_stmt_bind_result(stmt, &result_binds[0]))
		{
			mysql_stmt_close(stmt);
			return 6;
		}

		int ret = mysql_stmt_fetch(stmt);
		if (ret)
		{
			mysql_stmt_close(stmt);
			return 7;
		}

		for (u32 i = 0; i < field_num; i++)
		{
			Meta &meta = row[i];
			my_bool &isnull = isnull_vec[i];
			ulong &length = length_vec[i];

			if (isnull)
				meta = Meta();

			if (meta.is_string())
				meta.string_ref().resize(length);
		}

		mysql_stmt_close(stmt);
		return 0;
	}

	int DataSourceMysql::query_all(const string &sql, std::vector<Meta> &in, std::vector<std::vector<Meta>> &rows)
	{
		MYSQL_STMT *stmt = mysql_stmt_init(&_dbase);
		if (!stmt)
			return 1;

		if (mysql_stmt_prepare(stmt, sql.c_str(), sql.size()))
		{
			mysql_stmt_close(stmt);
			return 2;
		}

		std::vector<MYSQL_BIND> param_binds;
		std::vector<ulong> param_lens;

		if (!in.empty())
		{
			param_binds.resize(in.size());
			param_lens.resize(in.size());

			for (size_t i = 0; i < in.size(); i++)
			{
				MYSQL_BIND &bind = param_binds[i];
				memset(&bind, 0, sizeof(bind));
				ulong &len = param_lens[i];
				Meta &meta = in[i];

				if (meta.is_integer())
				{
					i32 &val = meta.integer_ref();

					bind.buffer_type = MYSQL_TYPE_LONG;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_bigint())
				{
					i64 &val = meta.bigint_ref();

					bind.buffer_type = MYSQL_TYPE_LONGLONG;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_float())
				{
					f32 &val = meta.float_ref();

					bind.buffer_type = MYSQL_TYPE_FLOAT;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_double())
				{
					f64 &val = meta.double_ref();

					bind.buffer_type = MYSQL_TYPE_DOUBLE;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_string())
				{
					string &val = meta.string_ref();
					len = val.size();

					bind.buffer_type = MYSQL_TYPE_STRING;
					bind.buffer = (char *)&val[0];
					bind.buffer_length = len;
					bind.length = &len;
				}
				else
				{
					bind.buffer_type = MYSQL_TYPE_NULL;
					bind.buffer = NULL;
				}
			}

			if (mysql_stmt_bind_param(stmt, &param_binds[0]))
			{
				mysql_stmt_close(stmt);
				return 3;
			}
		}

		if (mysql_stmt_execute(stmt))
		{
			mysql_stmt_close(stmt);
			return 4;
		}

		MYSQL_RES *result_meta = mysql_stmt_result_metadata(stmt);
		if (!result_meta)
		{
			mysql_stmt_close(stmt);
			return 5;
		}

		MYSQL_FIELD *fields = mysql_fetch_fields(result_meta);
		u32 field_num = mysql_num_fields(result_meta);
		mysql_free_result(result_meta);

		std::vector<MYSQL_BIND> result_binds;
		std::vector<Meta> row;
		std::vector<my_bool> isnull_vec;
		std::vector<ulong> length_vec;

		result_binds.resize(field_num);
		row.resize(field_num);
		isnull_vec.resize(field_num);
		length_vec.resize(field_num);

		for (u32 i = 0; i < field_num; i++)
		{
			MYSQL_BIND &bind = result_binds[i];
			memset(&bind, 0, sizeof(bind));

			MYSQL_FIELD &field = fields[i];
			Meta &meta = row[i];
			my_bool &isnull = isnull_vec[i];
			ulong &length = length_vec[i];

			if (field.type == MYSQL_TYPE_LONG)
			{
				meta = (i32)0;
				i32 &val = meta.integer_ref();

				bind.buffer_type = MYSQL_TYPE_LONG;
				bind.buffer = (char *)&val;
				bind.is_null = &isnull;
			}
			else if (field.type == MYSQL_TYPE_LONGLONG)
			{
				meta = (i64)0;
				i64 &val = meta.bigint_ref();

				bind.buffer_type = MYSQL_TYPE_LONGLONG;
				bind.buffer = (char *)&val;
				bind.is_null = &isnull;
			}
			else if (field.type == MYSQL_TYPE_FLOAT)
			{
				meta = (f32)0;
				f32 &val = meta.float_ref();

				bind.buffer_type = MYSQL_TYPE_FLOAT;
				bind.buffer = (char *)&val;
				bind.is_null = &isnull;
			}
			else if (field.type == MYSQL_TYPE_DOUBLE)
			{
				meta = (f64)0;
				f64 &val = meta.double_ref();

				bind.buffer_type = MYSQL_TYPE_DOUBLE;
				bind.buffer = (char *)&val;
				bind.is_null = &isnull;
			}
			else if (field.type == MYSQL_TYPE_STRING || field.type == MYSQL_TYPE_VAR_STRING)
			{
				meta = "";
				string &val = meta.string_ref();
				val.resize(field.length);

				bind.buffer_type = field.type;
				bind.buffer = (char *)&val[0];
				bind.buffer_length = val.size();
				bind.length = &length;
				bind.is_null = &isnull;
			}
		}

		if (mysql_stmt_bind_result(stmt, &result_binds[0]))
		{
			mysql_stmt_close(stmt);
			return 6;
		}

		while (!mysql_stmt_fetch(stmt))
		{
			for (u32 i = 0; i < field_num; i++)
			{
				Meta &meta = row[i];
				my_bool &isnull = isnull_vec[i];
				ulong &length = length_vec[i];

				if (isnull)
					meta = Meta();

				if (meta.is_string())
					meta.string_ref().resize(length);
			}

			rows.push_back(row);
		}

		mysql_stmt_close(stmt);
		return 0;
	}


	int DataSourceMysql::insert(const string &sql, std::vector<Meta> &in, i64 *insert_id)
	{
		MYSQL_STMT *stmt = mysql_stmt_init(&_dbase);
		if (!stmt)
			return 1;

		if (mysql_stmt_prepare(stmt, sql.c_str(), sql.size()))
		{
			mysql_stmt_close(stmt);
			return 2;
		}

		std::vector<MYSQL_BIND> param_binds;
		std::vector<ulong> param_lens;

		if (!in.empty())
		{
			param_binds.resize(in.size());
			param_lens.resize(in.size());

			for (size_t i = 0; i < in.size(); i++)
			{
				MYSQL_BIND &bind = param_binds[i];
				memset(&bind, 0, sizeof(bind));
				ulong &len = param_lens[i];
				Meta &meta = in[i];

				if (meta.is_integer())
				{
					i32 &val = meta.integer_ref();

					bind.buffer_type = MYSQL_TYPE_LONG;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_bigint())
				{
					i64 &val = meta.bigint_ref();

					bind.buffer_type = MYSQL_TYPE_LONGLONG;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_float())
				{
					f32 &val = meta.float_ref();

					bind.buffer_type = MYSQL_TYPE_FLOAT;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_double())
				{
					f64 &val = meta.double_ref();

					bind.buffer_type = MYSQL_TYPE_DOUBLE;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_string())
				{
					string &val = meta.string_ref();
					len = val.size();

					bind.buffer_type = MYSQL_TYPE_STRING;
					bind.buffer = (char *)&val[0];
					bind.buffer_length = len;
					bind.length = &len;
				}
				else
				{
					bind.buffer_type = MYSQL_TYPE_NULL;
					bind.buffer = NULL;
				}
			}

			if (mysql_stmt_bind_param(stmt, &param_binds[0]))
				return 3;
		}

		if (mysql_stmt_execute(stmt))
		{
			mysql_stmt_close(stmt);
			return 4;
		}

		if (insert_id)
			*insert_id = mysql_stmt_insert_id(stmt);

		mysql_stmt_close(stmt);
		return 0;
	}

	int DataSourceMysql::execute(const string &sql, std::vector<Meta> &in, i64 *affected)
	{
		MYSQL_STMT *stmt = mysql_stmt_init(&_dbase);
		if (!stmt)
			return 1;

		if (mysql_stmt_prepare(stmt, sql.c_str(), sql.size()))
		{
			mysql_stmt_close(stmt);
			return 2;
		}

		std::vector<MYSQL_BIND> param_binds;
		std::vector<ulong> param_lens;

		if (!in.empty())
		{
			param_binds.resize(in.size());
			param_lens.resize(in.size());

			for (size_t i = 0; i < in.size(); i++)
			{
				MYSQL_BIND &bind = param_binds[i];
				memset(&bind, 0, sizeof(bind));
				ulong &len = param_lens[i];
				Meta &meta = in[i];

				if (meta.is_integer())
				{
					i32 &val = meta.integer_ref();

					bind.buffer_type = MYSQL_TYPE_LONG;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_bigint())
				{
					i64 &val = meta.bigint_ref();

					bind.buffer_type = MYSQL_TYPE_LONGLONG;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_float())
				{
					f32 &val = meta.float_ref();

					bind.buffer_type = MYSQL_TYPE_FLOAT;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_double())
				{
					f64 &val = meta.double_ref();

					bind.buffer_type = MYSQL_TYPE_DOUBLE;
					bind.buffer = (char *)&val;
				}
				else if (meta.is_string())
				{
					string &val = meta.string_ref();
					len = val.size();

					bind.buffer_type = MYSQL_TYPE_STRING;
					bind.buffer = (char *)&val[0];
					bind.buffer_length = len;
					bind.length = &len;
				}
				else
				{
					bind.buffer_type = MYSQL_TYPE_NULL;
					bind.buffer = NULL;
				}
			}

			if (mysql_stmt_bind_param(stmt, &param_binds[0]))
			{
				mysql_stmt_close(stmt);
				return 3;
			}
		}

		if (mysql_stmt_execute(stmt))
		{
			mysql_stmt_close(stmt);
			return 4;
		}

		if (affected)
			*affected = mysql_stmt_affected_rows(stmt);

		mysql_stmt_close(stmt);
		return 0;
	}

	int DataSourceMysql::execute(const string &sql)
	{
		if (mysql_query(&_dbase, sql.c_str()))
		{
			return 1;
		}

		return 0;
	}

	u32 DataSourceMysql::last_errno()
	{
		return mysql_errno(&_dbase);
	}

	const char *DataSourceMysql::last_error()
	{
		return mysql_error(&_dbase);
	}

}
