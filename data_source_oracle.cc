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

#ifdef STDEX_HAS_ORACLE
#include "data_source_oracle.h"
namespace stdex {

DataSourceOracle::DataSourceOracle()
{
	OCI_Initialize(NULL, NULL, OCI_ENV_DEFAULT|OCI_ENV_THREADED|OCI_ENV_CONTEXT);
}

DataSourceOracle::~DataSourceOracle()
{
	close();
	OCI_Cleanup();
}

bool DataSourceOracle::is_ready()
{
	if (pool)
		return true;
	else
		return false;
}

int DataSourceOracle::open(const string &host, int port, const string &user, const string &passwd, const string &dbase)
{
	char buf[1024];
	snprintf(buf, sizeof(buf), "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d))(CONNECT_DATA=(SID=%s)))",
		host.c_str(), port, dbase.c_str());

	pool = OCI_PoolCreate(buf, user.c_str(), passwd.c_str(), OCI_POOL_SESSION, OCI_SESSION_DEFAULT, 1, 10, 1);
    if (!pool)
        return 1;

    OCI_PoolSetTimeout(pool, 30);  //todo
    OCI_PoolSetNoWait(pool, TRUE);
	return 0;
}

void DataSourceOracle::close()
{
	if (pool)
	{
		OCI_PoolFree(pool);
		pool = NULL;
	}
}

int DataSourceOracle::query(const string &sql, std::vector<Meta> &in, std::vector<Meta> &row)
{
	OCI_Connection *conn = OCI_PoolGetConnection(pool, NULL);
	if (!conn)
		return 1;

	OCI_Statement *stmt = OCI_StatementCreate(conn);
	if (!stmt)
	{
		OCI_ConnectionFree(conn);
		return 2;
	}

	if (!OCI_Prepare(stmt, sql.c_str()))
	{
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 3;
	}

	if (!in.empty())
	{
		for (size_t i=0; i<in.size(); i++)
		{
			Meta &meta = in[i];

			char pos[12];
			sprintf(pos, ":%d", i+1);

			if (meta.is_integer())
			{
				i32 &val = meta.integer_ref();
				OCI_BindInt(stmt, pos, &val);
			}
			else if (meta.is_bigint())
			{
				i64 &val = meta.bigint_ref();
				OCI_BindBigInt(stmt, pos, &val);
			}
			else if (meta.is_float())
			{
				f32 &val = meta.float_ref();
				OCI_BindFloat(stmt, pos, &val);
			}
			else if (meta.is_double())
			{
				f64 &val = meta.double_ref();
				OCI_BindDouble(stmt, pos, &val);
			}
			else if (meta.is_string())
			{
				string &val = meta.string_ref();
				ulong len = val.size();
				OCI_BindString(stmt, pos, &val[0], len);
			}
			else
			{
				OCI_StatementFree(stmt);
				OCI_ConnectionFree(conn);
				return 4;
			}
		}
	}

	if (!OCI_Execute(stmt))
	{
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 5;
	}

	OCI_Resultset *rs = OCI_GetResultset(stmt);
	if (!rs)
	{
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 6;
	}

	u32 field_num = OCI_GetColumnCount(rs);
	row.resize(field_num);

	if (!OCI_FetchNext(rs))
	{
		OCI_ReleaseResultsets(stmt);
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 7;
	}

	for (u32 i=0; i<field_num; i++)
	{
		if (!OCI_IsNull(rs, i+1))
		{
			OCI_Column *col = OCI_GetColumn(rs, i+1);
			u32 col_type = OCI_ColumnGetType(col);

			if (col_type == OCI_CDT_NUMERIC)
			{
				if (OCI_ColumnGetScale(col) <= 0)
				{
					if (OCI_ColumnGetPrecision(col) >= 10)
					{
						row[i] = (i64)OCI_GetBigInt(rs, i+1);
					}
					else
					{
						row[i] = (i32)OCI_GetInt(rs, i+1);
					}
				}
				else
				{
					if (OCI_ColumnGetPrecision(col) >= 10)
					{
						row[i] = (f32)OCI_GetDouble(rs, i+1);
					}
					else
					{
						row[i] = (f64)OCI_GetFloat(rs, i+1);
					}
				}
			}
			else if (col_type == OCI_CDT_TEXT)
			{
				row[i] = OCI_GetString(rs, i+1);
			}
		}
	}

    OCI_ReleaseResultsets(stmt);
    OCI_StatementFree(stmt);
    OCI_ConnectionFree(conn);
	return 0;
}

int DataSourceOracle::query_all(const string &sql, std::vector<Meta> &in, std::vector<std::vector<Meta>> &rows)
{
	OCI_Connection *conn = OCI_PoolGetConnection(pool, NULL);
	if (!conn)
		return 1;

	OCI_Statement *stmt = OCI_StatementCreate(conn);
	if (!stmt)
	{
		OCI_ConnectionFree(conn);
		return 2;
	}

	if (!OCI_Prepare(stmt, sql.c_str()))
	{
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 3;
	}

	if (!in.empty())
	{
		for (size_t i=0; i<in.size(); i++)
		{
			Meta &meta = in[i];

			char pos[12];
			sprintf(pos, ":%d", i+1);

			if (meta.is_integer())
			{
				i32 &val = meta.integer_ref();
				OCI_BindInt(stmt, pos, &val);
			}
			else if (meta.is_bigint())
			{
				i64 &val = meta.bigint_ref();
				OCI_BindBigInt(stmt, pos, &val);
			}
			else if (meta.is_float())
			{
				f32 &val = meta.float_ref();
				OCI_BindFloat(stmt, pos, &val);
			}
			else if (meta.is_double())
			{
				f64 &val = meta.double_ref();
				OCI_BindDouble(stmt, pos, &val);
			}
			else if (meta.is_string())
			{
				string &val = meta.string_ref();
				ulong len = val.size();
				OCI_BindString(stmt, pos, &val[0], len);
			}
			else
			{
				OCI_StatementFree(stmt);
				OCI_ConnectionFree(conn);
				return 4;
			}
		}
	}

	if (!OCI_Execute(stmt))
	{
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 5;
	}

	OCI_Resultset *rs = OCI_GetResultset(stmt);
	if (!rs)
	{
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 6;
	}

	u32 field_num = OCI_GetColumnCount(rs);
	std::vector<Meta> row;
	row.resize(field_num);

	while (OCI_FetchNext(rs))
	{
		for (u32 i=0; i<field_num; i++)
		{
			if (!OCI_IsNull(rs, i+1))
			{
				OCI_Column *col = OCI_GetColumn(rs, i+1);
				u32 col_type = OCI_ColumnGetType(col);

				if (col_type == OCI_CDT_NUMERIC)
				{
					if (OCI_ColumnGetScale(col) <= 0)
					{
						if (OCI_ColumnGetPrecision(col) >= 10)
						{
							row[i] = (i64)OCI_GetBigInt(rs, i+1);
						}
						else
						{
							row[i] = (i32)OCI_GetInt(rs, i+1);
						}
					}
					else
					{
						if (OCI_ColumnGetPrecision(col) >= 10)
						{
							row[i] = (f32)OCI_GetDouble(rs, i+1);
						}
						else
						{
							row[i] = (f64)OCI_GetFloat(rs, i+1);
						}
					}
				}
				else if (col_type == OCI_CDT_TEXT)
				{
					row[i] = OCI_GetString(rs, i+1);
				}
			}
		}

		rows.push_back(row);
	}

	OCI_ReleaseResultsets(stmt);
	OCI_StatementFree(stmt);
	OCI_ConnectionFree(conn);
	return 0;
}


int DataSourceOracle::insert(const string &sql, std::vector<Meta> &in)
{
	OCI_Connection *conn = OCI_PoolGetConnection(pool, NULL);
	if (!conn)
		return 1;

	OCI_Statement *stmt = OCI_StatementCreate(conn);
	if (!stmt)
	{
		OCI_ConnectionFree(conn);
		return 2;
	}

	if (!OCI_Prepare(stmt, sql.c_str()))
	{
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 3;
	}

	if (!in.empty())
	{
		for (size_t i=0; i<in.size(); i++)
		{
			Meta &meta = in[i];

			char pos[12];
			sprintf(pos, ":%d", i+1);

			if (meta.is_integer())
			{
				i32 &val = meta.integer_ref();
				OCI_BindInt(stmt, pos, &val);
			}
			else if (meta.is_bigint())
			{
				i64 &val = meta.bigint_ref();
				OCI_BindBigInt(stmt, pos, &val);
			}
			else if (meta.is_float())
			{
				f32 &val = meta.float_ref();
				OCI_BindFloat(stmt, pos, &val);
			}
			else if (meta.is_double())
			{
				f64 &val = meta.double_ref();
				OCI_BindDouble(stmt, pos, &val);
			}
			else if (meta.is_string())
			{
				string &val = meta.string_ref();
				ulong len = val.size();
				OCI_BindString(stmt, pos, &val[0], len);
			}
			else
			{
				OCI_StatementFree(stmt);
				OCI_ConnectionFree(conn);
				return 4;
			}
		}
	}

	if (!OCI_Execute(stmt))
	{
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 5;
	}

	OCI_StatementFree(stmt);
	OCI_ConnectionFree(conn);
	return 0;
}

int DataSourceOracle::execute(const string &sql, std::vector<Meta> &in, i64 *affected)
{
	OCI_Connection *conn = OCI_PoolGetConnection(pool, NULL);
	if (!conn)
		return 1;

	OCI_Statement *stmt = OCI_StatementCreate(conn);
	if (!stmt)
	{
		OCI_ConnectionFree(conn);
		return 2;
	}

	if (!OCI_Prepare(stmt, sql.c_str()))
	{
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 3;
	}

	if (!in.empty())
	{
		for (size_t i=0; i<in.size(); i++)
		{
			Meta &meta = in[i];

			char pos[12];
			sprintf(pos, ":%d", i+1);

			if (meta.is_integer())
			{
				i32 &val = meta.integer_ref();
				OCI_BindInt(stmt, pos, &val);
			}
			else if (meta.is_bigint())
			{
				i64 &val = meta.bigint_ref();
				OCI_BindBigInt(stmt, pos, &val);
			}
			else if (meta.is_float())
			{
				f32 &val = meta.float_ref();
				OCI_BindFloat(stmt, pos, &val);
			}
			else if (meta.is_double())
			{
				f64 &val = meta.double_ref();
				OCI_BindDouble(stmt, pos, &val);
			}
			else if (meta.is_string())
			{
				string &val = meta.string_ref();
				ulong len = val.size();
				OCI_BindString(stmt, pos, &val[0], len);
			}
			else
			{
				OCI_StatementFree(stmt);
				OCI_ConnectionFree(conn);
				return 4;
			}
		}
	}

	if (!OCI_Execute(stmt))
	{
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 5;
	}

	if (affected)
		*affected = OCI_GetAffectedRows(stmt);

	OCI_StatementFree(stmt);
	OCI_ConnectionFree(conn);
	return 0;
}


int DataSourceOracle::execute(const string &sql)
{
	OCI_Connection *conn = OCI_PoolGetConnection(pool, NULL);
	if (!conn)
		return 1;

	OCI_Statement *stmt = OCI_StatementCreate(conn);
	if (!stmt)
	{
		OCI_ConnectionFree(conn);
		return 2;
	}

	if (!OCI_ExecuteStmt(stmt, sql.c_str()))
	{
		OCI_StatementFree(stmt);
		OCI_ConnectionFree(conn);
		return 3;
	}

	OCI_StatementFree(stmt);
	OCI_ConnectionFree(conn);
	return 0;
}

u32 DataSourceOracle::last_errno()
{
	OCI_Error *err = OCI_GetLastError();
	return err ? OCI_ErrorGetOCICode(err) : -1;
}

const char *DataSourceOracle::last_error()
{
	OCI_Error *err = OCI_GetLastError();
	return err ? OCI_ErrorGetString(err) : "unknown ocilib error";
}

}
#endif