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

class RwRow;
class RdOnlyRow;

class RowCompare {
public:
	RowCompare(TableSchema *schema) :
			s_(schema) {
	}
	bool operator()(char * const r1, char * const r2) const;
private:
	TableSchema *s_;
};

class MemTable {
public:
	MemTable(TableSchema *schema);
	~MemTable();

	bool InsertRow(RwRow &row, bool update = true);

	void Clear();
	void PrintAll();

	TableSchema *GetSchema() {
		return schema_;
	}

	//iterate the contents of in-memory sorted list, adapted from leveldb's skiplist iterator

	class Iterator {
	public:
		explicit Iterator(MemTable* table);

		// Returns true iff the iterator is positioned at a valid node.
		bool Valid();

		//returns true iff the iterator's current node has matching index key
		template<class T> bool Valid(const T &key);
		//returns true iff the iterator's current node has matching index and primary key
		template<class T, class U> bool Valid(const T &key, const U &primary);

		// Returns the row at the current position.
		// REQUIRES: Valid()
		RdOnlyRow & RowAt(RdOnlyRow &r);

		// Advances to the next next.
		// REQUIRES: Valid()
		void Next();

		// Advances to the previous row.
		// REQUIRES: Valid()
		void Prev();

		// Advance to the first row entry whose index+primary key is >= target
		template<class T> void Seek(const T &key);
		template<class T, class U> void Seek(const T &key,
				const U &primary);
		void SeekRow(RdOnlyRow &r);

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

class RdOnlyRow {
public:
	RdOnlyRow(TableSchema *s, char *b=NULL) : schema_(s), buf_(b) {}
	RdOnlyRow(MemTable *table, char *b=NULL) :
			schema_(table->GetSchema()), buf_(b) {
	}
	~RdOnlyRow() {
	}
	char *Buffer() {
		return buf_;
	}

	char * ReplaceRowBuffer(char *b) {
		char *tmp = buf_;
		buf_ = b;
		return tmp;
	}

	//cannot overload function with return types, so have to use different names
	int GetIntColumn(int colno);
	std::string GetStrColumn(int colno);
	void GetColumn(int colno, int *ret);
	void GetColumn(int colno, std::string *ret);

	static bool LessThan(char * const r1, char * const r2, TableSchema *s);
	void PrintRow();
protected:
	char *buf_;
	TableSchema *schema_;
};

class RwRow : public RdOnlyRow {
public:
	RwRow(TableSchema *s);
	RwRow(MemTable *table);
	~RwRow();

	void PutColumn(const int & x, int colno);
	void PutColumn(const std::string & s, int colno);

	template<class T> void AddColumn(const T &x);

private:
	void AllocBuffer();
	int col_;
};


RwRow& operator<<(RwRow &, const int &c);
RwRow& operator<<(RwRow &, const std::string &s);

template<class T> void RwRow::AddColumn(const T &val) {
	PutColumn(val, col_);
	col_++;
}

template<class T> void MemTable::Iterator::Seek(const T &key) {
	RwRow r(table_);
	r.PutColumn(key, table_->GetSchema()->GetIndexNumber());
	return SeekRow(r);
}

template<class T, class U> void MemTable::Iterator::Seek(const T &key,
		const U &primary) {
	RwRow r(table_);
	r.PutColumn(key, table_->GetSchema()->GetIndexNumber());
	r.PutColumn(primary, table_->GetSchema()->GetPrimaryNumber());
	return SeekRow(r);
}

template<class T> bool MemTable::Iterator::Valid(const T &key) {
	if (iter_ == table_->content_->end())
		return false;
	T t1;
	RdOnlyRow r(table_,iter_->first);
	r.GetColumn(table_->GetSchema()->GetIndexNumber(), &t1);
	if (t1 > key)
		return false;
	return true;
}

template<class T, class U> bool MemTable::Iterator::Valid(const T &key,
		const U &primary) {
	if (iter_ == table_->content_->end())
		return false;
	T t1;
	RdOnlyRow r(table_, iter_->first);
	r.GetColumn(table_->GetSchema()->GetIndexNumber(), &t1);
	if (t1 > key)
		return false;
	U t2;
	r.ReplaceRowBuffer(iter_->first);
	r.GetColumn(table_->GetSchema()->GetPrimaryNumber(), &t2);
	if (t2 > primary)
		return false;
	return true;
}

} //namespace memdb

#endif
