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
	bool is_ready() const;

	int open(const string &host, int port, const string &user, const string &passwd, const string &dbase);
	void close();

	int query(const string &sql, std::vector<Meta> &in, std::vector<Meta> &row);
	int query_all(const string &sql, std::vector<Meta> &in, std::vector<std::vector<Meta>> &rows);
	int insert(const string &sql, std::vector<Meta> &in);
	int execute(const string &sql, std::vector<Meta> &in, i64 *affected);
	int execute(const string &sql);

	unsigned last_errno() const;
	const char *last_error() const;

	int get_magic() const;
	void set_magic(int v);

private:
	OCI_ConnPool *pool;
	int _magic;
};

}
#endif
#endif //STDEX_DATA_SOURCE_ORACLE_H_
