#ifndef STDEX_DATA_SOURCE_ORACLE_H_
#define STDEX_DATA_SOURCE_ORACLE_H_
#ifdef STDEX_HAS_ORACLE

#include "data_meta.h"
#include <ocilib.h>
namespace stdex {

class DataSourceOracle
{
public:
	DataSourceOracle();
	~DataSourceOracle();
	bool is_ready();

	int open(const string &host, int port, const string &user, const string &passwd, const string &dbase);
	void close();

	int query(const string &sql, std::vector<Meta> &in, std::vector<Meta> &row);
	int query_all(const string &sql, std::vector<Meta> &in, std::vector<std::vector<Meta>> &rows);
	int insert(const string &sql, std::vector<Meta> &in);
	int execute(const string &sql, std::vector<Meta> &in, i64 *affected);
	int execute(const string &sql);

	u32 last_errno();
	const char *last_error();

private:
	OCI_ConnPool *pool;
};

}
#endif
#endif //STDEX_DATA_SOURCE_ORACLE_H_