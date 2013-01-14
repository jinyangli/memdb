/*
 * memtable.cc
 *
 *  Created on: Jan 3, 2013
 *      Author: jinyang
 */

#include <stdlib.h>
#include <string.h>
#include <string>
#include <assert.h>
#include <map>
#include "db/memtable.h"

namespace memdb {

bool RowCompare::operator()(char * const r1, char * const r2) const {
	return Row::LessEqual(r1, r2, s_);
}

MemTable::MemTable(TableSchema *schema) :
		schema_(schema) {
	RowCompare compr(schema_);
	content_ = new std::map<char *, int, RowCompare>(compr);
	assert(content_);
}

bool MemTable::InsertRow(Row &row) {
	bool update = true;
	auto it = content_->lower_bound(row.Buffer());
	if (it!=content_->end() && Row::LessEqual(it->first, row.Buffer(),schema_)) {
		if (update) {
			Row::FreeRow(it->first, schema_);
			content_->erase(it++);
		}else {
			return false;
		}
	}
	content_->insert(it, std::pair<char *, int>(row.TakeBuf(), 1));
	return true;
}

void MemTable::Clear() {
	for (auto it = content_->begin(); it != content_->end(); ++it) {
		Row::FreeRow(it->first, schema_);
	}
	content_->clear();
}

void MemTable::PrintAll() {
	for (int i = 0; i < schema_->NumColumns(); i++) {
		printf("%s\t", schema_->GetColumnName(i).c_str());
	}
	printf("\n");
	for (auto it = content_->begin(); it != content_->end(); ++it) {
		Row::PrintRow(it->first, schema_);
	}
}

/* --------------------------- Row ----------------------------------*/

Row::Row(MemTable *table) :
		schema_(table->GetSchema()), col_(0) {
	buf_ = (char *) malloc(schema_->RowByteSize());
	assert(buf_);
	bzero(buf_, schema_->RowByteSize());
	assert(buf_);
}

Row::~Row() {
	if (buf_) {
		Row::FreeRow(buf_, schema_);
	}
	buf_ = NULL;
}
/*
 Row::Row(TableSchema *schema, char *buf) : schema_(schema), buf_(buf)  {
 assert(buf);
 col_ = schema_->NumColumns()-1;
 }
 */
Row &
operator<<(Row &r, const int & x) {
	r.PutInt(x);
	return r;
}

Row &
operator<<(Row &r, const std::string &s) {
	r.PutString(s);
	return r;
}

void Row::PutIntAtColumn(const int &x, const int &colno) {
	assert(schema_->GetColumnType(colno) == cInt32);
	*((int *) (buf_ + schema_->GetColumnPos(colno))) = x;
}

void Row::PutInt(const int &x) {
	PutIntAtColumn(x, col_);
	col_++;
}

void Row::PutStringAtColumn(const std::string &s, const int &colno) {
	assert(schema_->GetColumnType(colno) == cString);
	char *cs = (char *) malloc(s.length() + 1);
	assert(cs);
	memcpy(cs, s.c_str(), s.length() + 1);
	*((char **) (buf_ + schema_->GetColumnPos(colno))) = cs;
}

void Row::PutString(const std::string &s) {
	PutStringAtColumn(s, col_);
	col_++;
}

int Row::GetInt(RdOnlyRow& r, TableSchema *s, const std::string & col) {
	int colno = s->GetColumnNumber(col);
	assert(s->GetColumnType(colno) == cInt32);
	return *(int *) (r + s->GetColumnPos(colno));
}

std::string Row::GetString(RdOnlyRow& r, TableSchema *s,
		const std::string & col) {
	int colno = s->GetColumnNumber(col);
	assert(s->GetColumnType(colno) == cString);
	return std::string(*(char **)(r + s->GetColumnPos(colno)));
}

bool Row::LessEqual(RdOnlyRow &r1, RdOnlyRow &r2, TableSchema *s) {
	int pos = s->GetIndexPos();
	if (s->GetIndexType() == cInt32) {
		if ((*(int *) r1 + pos) > (*(int *) r2 + pos)) {
			return false;
		} else if ((*(int *) (r1 + pos)) < (*(int *) (r2 + pos))) {
			return true;
		}
	} else if (s->GetIndexType() == cString) {
		int result = strcmp(*(char **) (r1 + pos), *(char **) (r2 + pos));
		if (result > 0) {
			return false;
		} else if (result < 0) {
			return true;
		}
	} else {
		assert(0);
	}

	if (pos == s->GetPrimaryPos()) {
		return true;
	} else {
		pos = s->GetPrimaryPos();
	}

	if (s->GetPrimaryType() == cInt32) {
		if ((*(int *) (r1 + pos)) > (*(int *) (r2 + pos)))
			return false;
	} else if (s->GetPrimaryType() == cString) {
		int result = strcmp(*(char **) (r1 + pos), *(char **) (r2 + pos));
		if (result > 0)
			return false;
	} else {
		assert(0);
	}
	return true;
}

void Row::PrintRow(RdOnlyRow &r, TableSchema *s) {
	for (int i = 0; i < s->NumColumns(); i++) {
		if (s->GetColumnType(i) == cString) {
			printf("%s\t", *(char **) (r + s->GetColumnPos(i)));
		} else if (s->GetColumnType(i) == cInt32) {
			printf("%d\t", *(int *) (r + s->GetColumnPos(i)));
		}
	}
	printf("\n");
}

void Row::FreeRow(char *r, TableSchema *s) {
	for (int i = 0; i < s->NumColumns(); i++) {
		if (s->GetColumnType(i) == cString) {
			free(*(char **) (r + s->GetColumnPos(i)));
		}
	}
	free(r);
}

/*-----------------MemTable::Iterator---------------*/
MemTable::Iterator::Iterator(MemTable* table) :
		table_(table) {
	iter_ = table_->content_->begin();
}

bool MemTable::Iterator::Valid() {
	return (iter_ != table_->content_->end());
}

RdOnlyRow &
MemTable::Iterator::RowAt() {
	return iter_->first;
}

void MemTable::Iterator::Next() {
	iter_++;
}

void MemTable::Iterator::Prev() {
	iter_--;
}

RdOnlyRow &
MemTable::Iterator::Seek(const int &key) {
	Row r(table_);
	r.PutIntAtColumn(key, table_->GetSchema()->GetIndexNumber());
	return Seek(r);

}

RdOnlyRow&
MemTable::Iterator::Seek(const std::string &key) {

	Row r(table_);
	r.PutStringAtColumn(key, table_->GetSchema()->GetIndexNumber());
	return Seek(r);

}

RdOnlyRow&
MemTable::Iterator::Seek(Row &r) {
	iter_ = table_->content_->lower_bound(r.Buffer());
}

void MemTable::Iterator::SeekToFirst() {
	iter_ = table_->content_->begin();
}

} //namespace memdb
