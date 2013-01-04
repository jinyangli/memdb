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

bool RowCompare::operator()(const char *r1, const char *r2) const {
	int pos = s_->GetIndexPos();
	if (s_->GetIndexType() == cInt32) {
		if ((*(int *) r1 + pos) > (*(int *) r2 + pos)) {
			return false;
		} else if ((*(int *) (r1 + pos)) < (*(int *) (r2 + pos))) {
			return true;
		}
	} else if (s_->GetIndexType() == cString) {
		int result = strcmp(*(char **) (r1 + pos), *(char **) (r2 + pos));
		if (result > 0) {
			return false;
		} else if (result < 0) {
			return true;
		}
	} else {
		assert(0);
	}

	if (pos == s_->GetPrimaryPos()) {
		return true;
	} else {
		pos = s_->GetPrimaryPos();
	}

	if (s_->GetPrimaryType() == cInt32) {
		if ((*(int *) (r1 + pos)) > (*(int *) (r2 + pos)))
			return false;
	} else if (s_->GetPrimaryType() == cString) {
		int result = strcmp(*(char **) (r1 + pos), *(char **) (r2 + pos));
		if (result > 0)
			return false;
	} else {
		assert(0);
	}

	return true;
}

MemTable::MemTable(TableSchema *schema) :
		schema_(schema) {
	RowCompare compr(schema_);
	content_ = new std::map<char *, int, RowCompare>(compr);
	assert(content_);
}

void MemTable::InsertRow(Row &row) {
	content_->insert(std::pair<char *, int>(row.TakeBuf(), 1));
}

void MemTable::Clear() {
	for (auto it = content_->begin(); it!= content_->end(); ++it) {
		for (int i = 0; i < schema_->NumColumns(); i++) {
			if (schema_->GetColumnType(i) == cString) {
				free(*(char **)(it->first+schema_->GetColumnPos(i)));
			}
		}
		free(it->first);
	}
	content_->clear();
}

void MemTable::PrintAll() {
	for (int i = 0; i < schema_->NumColumns(); i++) {
		printf("%s\t", schema_->GetColumnName(i).c_str());
	}
	printf("\n");
	for (auto it = content_->begin(); it != content_->end(); ++it) {
		for (int i = 0; i < schema_->NumColumns(); i++) {
			if (schema_->GetColumnType(i) == cString) {
				printf("%s\t", *(char **)(it->first+schema_->GetColumnPos(i)));
			}else if (schema_->GetColumnType(i) == cInt32) {
				printf("%d\t", *(int *)(it->first+schema_->GetColumnPos(i)));
			}
		}
		printf("\n");
	}
}

Row::Row(TableSchema *schema) :
		schema_(schema), col_(0) {
		buf_ = (char *) malloc(schema_->RowByteSize());
		assert(buf_);
}

Row::Row(TableSchema *schema, char *buf) : schema_(schema), buf_(buf)  {
	assert(buf);
	col_ = schema_->NumColumns()-1;
}

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

void Row::PutInt(const int &x) {
	assert(schema_->GetColumnType(col_) == cInt32);
	*((int *) (buf_ + schema_->GetColumnPos(col_))) = x;
	col_++;
}

void Row::PutString(const std::string &s) {
	assert(schema_->GetColumnType(col_) == cString);
	char *cs = (char *)malloc(s.length() + 1);
	memcpy(cs, s.c_str(), s.length()+1);
	*((char **) (buf_ + schema_->GetColumnPos(col_))) = cs;
	col_++;
}

} //namespace memdb
