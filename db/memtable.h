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
typedef char * const RdOnlyRow;

class RowCompare {
public:
	RowCompare(TableSchema *schema) :
			s_(schema) {
	}
	bool operator()( char * const r1, char * const r2) const;
private:
	TableSchema *s_;
};

class MemTable {
public:
	MemTable(TableSchema *schema);
	~MemTable();

//	bool InsertRow(Row &row, bool update=true);
	bool InsertRow(Row &row);

	void Clear();
	void PrintAll();
	//void QueryRow(Slice index, Slice primary);
	//Slice *GetColumn(Slice *row, int col);
	TableSchema *GetSchema() {
		return schema_;
	}

	//iterate the contents of in-memory sorted list, adapted from leveldb's skiplist iterator

	class Iterator {
	public:
		explicit Iterator(MemTable* table);

		// Returns true iff the iterator is positioned at a valid node.
		bool Valid();

		// Returns the row at the current position.
		// REQUIRES: Valid()
		RdOnlyRow& RowAt();

		// Advances to the next next.
		// REQUIRES: Valid()
		void Next();

		// Advances to the previous row.
		// REQUIRES: Valid()
		void Prev();

		// Advance to the first row entry whose index+primary key is >= target
		RdOnlyRow & Seek(const int &key);
		RdOnlyRow & Seek(const std::string &key);
		RdOnlyRow & Seek(Row &r);

		// Position at the first entry in list.
		// Final state of iterator is Valid() iff list is not empty.
		void SeekToFirst();

	private:
		MemTable* table_;
		std::map<char *, int, RowCompare>::iterator iter_;
	};
private:
	TableSchema *schema_;
	std::map<char *, int, RowCompare> *content_;
};


class Row {
public:
	Row(MemTable *table);
	//Row(TableSchema *schema, char *buf);
	~Row();

	void PutInt(const int & x);
	void PutIntAtColumn(const int &x, const int &colno);
	void PutString(const std::string & s);
	void PutStringAtColumn(const std::string &x, const int &colno);

	char *TakeBuf() {
		char *tmpbuf = buf_;
		buf_ = NULL;
		return tmpbuf;
	}
	char *Buffer() {
		return  buf_;
	}

	//a bunch of helper functions for manipulating read-only rows
	static int GetInt(RdOnlyRow& r, TableSchema *schema, const std::string & col);
	static std::string GetString(RdOnlyRow& r, TableSchema *schema, const std::string & col);
	static inline bool LessEqual(RdOnlyRow &r1, RdOnlyRow &r2, TableSchema *schema);
	static void PrintRow(RdOnlyRow &r, TableSchema *schema);
	static void FreeRow(char *r, TableSchema *schema);

private:
	int col_;
	char *buf_;
	TableSchema *schema_;
};

Row& operator<<(Row &, const int &c);
Row& operator<<(Row &, const std::string &s);

} //namespace memdb

#endif
