#ifndef CCLIENT_DATA_SOURCE_MYSQL_H_
#define CCLIENT_DATA_SOURCE_MYSQL_H_

#include "data_meta.h"
#include <mysql.h>
namespace cclient {

class DataSourceMysql
{
public:
	DataSourceMysql();
	~DataSourceMysql();
	bool is_ready();

	int open(const string &host, int port, const string &user, const string &passwd, const string &dbase);
	void close();

	int query(const string &sql, std::vector<Meta> &in, std::vector<Meta> &row);
	int query_all(const string &sql, std::vector<Meta> &in, std::vector<std::vector<Meta>> &rows);
	int insert(const string &sql, std::vector<Meta> &in, i64 *insert_id=NULL);
	int execute(const string &sql, std::vector<Meta> &in, i64 *affected=NULL);
	int execute(const string &sql);

	u32 last_errno();
	const char *last_error();

private:
	MYSQL _dbase;
	bool _ready;
};

}
#endif //CCLIENT_DATA_SOURCE_MYSQL_H_