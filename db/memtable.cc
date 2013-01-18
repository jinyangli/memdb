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
	RdOnlyRow::LessThan(r1,r2,s_);
}

MemTable::MemTable(TableSchema *schema) :
		schema_(schema) {
	RowCompare compr(schema_);
	content_ = new std::map<char *, int, RowCompare>(compr);
	assert(content_);
}

bool MemTable::InsertRow(RwRow &r, bool update) {
	//bool update = true;
	auto it = content_->lower_bound(r.Buffer());
	if (it!=content_->end() && RdOnlyRow::LessThan(it->first, r.Buffer(), schema_)) {
		if (update) {
			schema_->FreeRowBuffer(it->first);
			content_->erase(it++);
		}else {
			return false;
		}
	}
	content_->insert(it, std::pair<char *, int>(r.ReplaceRowBuffer(NULL), 1));
	return true;
}

void MemTable::Clear() {
	for (auto it = content_->begin(); it != content_->end(); ++it) {
		schema_->FreeRowBuffer(it->first);
	}
	content_->clear();
}

void MemTable::PrintAll() {
	for (int i = 0; i < schema_->NumColumns(); i++) {
		printf("%s\t", schema_->GetColumnName(i).c_str());
	}
	printf("\n");
	RdOnlyRow r(schema_);
	for (auto it = content_->begin(); it != content_->end(); ++it) {
		r.ReplaceRowBuffer(it->first);
		r.PrintRow();
	}
}

/*-----------------MemTable::Iterator---------------*/
MemTable::Iterator::Iterator(MemTable* table) :
		table_(table) {
	iter_ = table_->content_->begin();
}

bool MemTable::Iterator::Valid() {
	return (iter_ != table_->content_->end());
}

RdOnlyRow&
MemTable::Iterator::RowAt(RdOnlyRow &r) {
	r.ReplaceRowBuffer(iter_->first);
	return r;
}

void MemTable::Iterator::Next() {
	iter_++;
}

void MemTable::Iterator::Prev() {
	iter_--;
}

void
MemTable::Iterator::SeekRow(RdOnlyRow &r) {
	iter_ = table_->content_->lower_bound(r.Buffer());
}

void MemTable::Iterator::SeekToFirst() {
	iter_ = table_->content_->begin();
}

/* --------------------------- RdOnlyRow ----------------------------------*/

int RdOnlyRow::GetIntColumn(int colno) {
	assert(schema_->GetColumnType(colno) == cInt32);
	return *(int *) (buf_ + schema_->GetColumnPos(colno));
}

void RdOnlyRow::GetColumn(int colno, int *ret) {
	*ret = GetIntColumn(colno);
}

std::string RdOnlyRow::GetStrColumn(int colno) {
	assert(schema_->GetColumnType(colno) == cString);
	return std::string(*(char **)(buf_ + schema_->GetColumnPos(colno)));
}

void RdOnlyRow::GetColumn(int colno, std::string *ret) {
	*ret = GetStrColumn(colno);
}

bool RdOnlyRow::LessThan(char * const r1, char * const r2, TableSchema *s) {
	int pos = s->GetIndexPos();
	if (s->GetIndexType() == cInt32) {
		if ((*(int *) r1 + pos) < (*(int *) r2 + pos)) {
			return true;
		} else if ((*(int *) (r1 + pos)) > (*(int *) (r2 + pos))) {
			return false;
		}
	} else if (s->GetIndexType() == cString) {
		int result = strcmp(*(char **) (r1 + pos), *(char **) (r2 + pos));
		if (result < 0) {
			return true;
		} else if (result > 0) {
			return false;
		}
	} else {
		assert(0);
	}

	if (pos == s->GetPrimaryPos()) {
		return false;
	} else {
		pos = s->GetPrimaryPos();
	}

	if (s->GetPrimaryType() == cInt32) {
		if ((*(int *) (r1 + pos)) < (*(int *) (r2 + pos)))
			return true;
	} else if (s->GetPrimaryType() == cString) {
		int result = strcmp(*(char **) (r1 + pos), *(char **) (r2 + pos));
		if (result < 0)
			return true;
	} else {
		assert(0);
	}
	return false;
}

void RdOnlyRow::PrintRow() {
	for (int i = 0; i < schema_->NumColumns(); i++) {
		if (schema_->GetColumnType(i) == cString) {
			printf("%s\t", *(char **) (buf_ + schema_->GetColumnPos(i)));
		} else if (schema_->GetColumnType(i) == cInt32) {
			printf("%d\t", *(int *) (buf_ + schema_->GetColumnPos(i)));
		}
	}
	printf("\n");
}


/*----------------------RwRow-------------------------------------------------*/
RwRow::RwRow(TableSchema *s) : RdOnlyRow(s), col_(0) {
	buf_ = schema_->AllocRowBuffer();
}
RwRow::RwRow(MemTable *t) : RdOnlyRow(t), col_(0) {
	buf_ = schema_->AllocRowBuffer();
}

RwRow::~RwRow() {
	if (buf_) {
		schema_->FreeRowBuffer(buf_);
		buf_ = NULL;
	}
}

RwRow &
operator<<(RwRow &r, const int & x) {
	r.AddColumn(x);
	return r;
}

RwRow &
operator<<(RwRow &r, const std::string &s) {
	r.AddColumn(s);
	return r;
}


void RwRow::PutColumn(const int &x, int colno) {
	assert(schema_->GetColumnType(colno) == cInt32);
	*((int *) (buf_ + schema_->GetColumnPos(colno))) = x;
}

void RwRow::PutColumn(const std::string &s, int colno) {
	assert(schema_->GetColumnType(colno) == cString);
	char *cs = (char *) malloc(s.length() + 1);
	assert(cs);
	memcpy(cs, s.c_str(), s.length() + 1);
	*((char **) (buf_ + schema_->GetColumnPos(colno))) = cs;
}



} //namespace memdb
