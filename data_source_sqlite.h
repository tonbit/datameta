#ifndef STDEX_DATA_SOURCE_SQLITE_H_
#define STDEX_DATA_SOURCE_SQLITE_H_
#ifdef STDEX_HAS_SQLITE

#include "data_meta.h"
#include <sqlite3.h>
namespace stdex {

class DataSourceSqlite
{
public:
	DataSourceSqlite();
	~DataSourceSqlite();

	int open(const string &filename);
	void close();
	bool is_ready() const;

	int query(const string &sql, std::vector<Meta> &in, std::vector<Meta> &row);
	int query_all(const string &sql, std::vector<Meta> &in, std::vector<std::vector<Meta>> &rows);
	int insert(const string &sql, std::vector<Meta> &in, i64 *insert_id=NULL);
	int execute(const string &sql, std::vector<Meta> &in, i64 *affected=NULL);
	int execute(const string &sql);

	unsigned last_errno() const;
	const char *last_error() const;

	int get_magic() const;
	void set_magic(int v);

private:
	sqlite3 *db;
	int _magic;
};

}
#endif
#endif //STDEX_DATA_SOURCE_SQLITE_H_
