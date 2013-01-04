/*
 * memtable.h
 *
 *  Created on: Dec 28, 2012
 *      Author: jinyang
 */

#ifndef MEMDB_DB_MEMTABLE_H_
#define MEMDB_DB_MEMTABLE_H_

#include "db/tableschema.h"
#include <map>

namespace memdb {

class Row;

class RowCompare {
public:
	RowCompare(TableSchema *schema) :
			s_(schema) {
	}
	bool operator()(const char *r1, const char *r2) const;
private:
	TableSchema *s_;
};

class MemTable {
public:
	MemTable(TableSchema *schema);
	~MemTable();

	void InsertRow(Row &row);
	void Clear();
	void PrintAll();
	//void QueryRow(Slice index, Slice primary);
	//Slice *GetColumn(Slice *row, int col);
	TableSchema *GetSchema() {
		return schema_;
	}
private:
	TableSchema *schema_;
	std::map<char *, int, RowCompare> *content_;
};

class Row {
public:
	Row(TableSchema *schema);
	Row(TableSchema *schema, char *buf);
	~Row() {}
	void PutInt(const int & x);
	void PutString(const std::string & s);
	char *TakeBuf() {
		return buf_;
	}

private:
	int col_;
	char *buf_;
	TableSchema *schema_;
};

Row& operator<<(Row &, const int &c);
Row& operator<<(Row &, const std::string &s);

} //namespace memdb

#endif
