#ifndef CCLIENT_DATA_SOURCE_SQLITE_H_
#define CCLIENT_DATA_SOURCE_SQLITE_H_

#include "data_meta.h"
#include <sqlite3.h>
namespace cclient {

class DataSourceSqlite
{
public:
	DataSourceSqlite();
	~DataSourceSqlite();

	int open(const string &filename);
	void close();
	bool is_ready();

	int query(const string &sql, std::vector<Meta> &in, std::vector<Meta> &row);
	int query_all(const string &sql, std::vector<Meta> &in, std::vector<std::vector<Meta>> &rows);
	int insert(const string &sql, std::vector<Meta> &in, i64 *insert_id=NULL);
	int execute(const string &sql, std::vector<Meta> &in, i64 *affected=NULL);
	int execute(const string &sql);

	u32 last_errno();
	const char *last_error();

private:
	sqlite3 *db;
};

}
#endif //CCLIENT_DATA_SOURCE_SQLITE_H_