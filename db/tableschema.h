/*
 * tableschema.h
 *
 *  Created on: Dec 28, 2012
 *      Author: jinyang
 */

#ifndef MEMDB_DB_TABLESCHEMA_H_
#define MEMDB_DB_TABLESCHEMA_H_

#include <vector>
#include <string>

namespace memdb {

typedef enum {
	cInt32 = 0, cString = 1 /*deal with only two types first*/
} column_t;

class TableSchema {
public:

	TableSchema(int NumColumns, const std::string cnames[], const column_t ctypes[], std::string primary);
	TableSchema(const std::vector<std::string> & cnames, const std::vector<column_t> &ctypes, std::string primary);
	~TableSchema();
	void init(const std::vector<std::string> & cnames, const std::vector<column_t> &ctypes, std::string primary);

	int NumColumns() {
		return ctypes_.size();
	}

	char *AllocRowBuffer();
	void FreeRowBuffer(char *buf);

	column_t GetColumnType(int c) {
		return ctypes_[c];
	}
	std::string GetColumnName(int c) {
		return cnames_[c];
	}

	int GetColumnNumber(std::string s);
	int GetColumnPos(int c) {
		return cpos_[c];
	}

	int GetIndexNumber() {
		return 0;
	}

	int GetIndexPos() {
		return cpos_[GetIndexNumber()];
	}

	column_t GetIndexType() {
		return ctypes_[0];
	}

	int GetPrimaryNumber() {
		return primary_;
	}

	int GetPrimaryPos() {
		return cpos_[primary_];
	}
	column_t GetPrimaryType() {
		return ctypes_[primary_];
	}

private:
	std::vector<column_t> ctypes_;
	std::vector<std::string> cnames_;
	std::vector<int> cpos_;
	int row_byte_sz_;
	int primary_;

	static const int cTypeToSize[2];
};

} //namespace memdb

#endif
