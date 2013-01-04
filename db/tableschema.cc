/*
 * tableschema.cc
 *
 *  Created on: Jan 2, 2013
 *      Author: jinyang
 */

#include "tableschema.h"
#include "assert.h"

#include <string>

namespace memdb {

const int TableSchema::cTypeToSize[2] = { sizeof(int), sizeof(void *) };

TableSchema::TableSchema(int NumColumns, const std::string cnames[],
		const column_t ctypes[], std::string primary_column) {
	std::vector<std::string> vname;
	std::vector<column_t> vtype;
	for (int i = 0; i < NumColumns; i++) {
		vname.push_back(cnames[i]);
		vtype.push_back(ctypes[i]);
	}
	init(vname,vtype,primary_column);
}

TableSchema::TableSchema(const std::vector<std::string> &cnames,
		const std::vector<column_t> &ctypes, std::string primary_column) {

	init(cnames, ctypes, primary_column);
}

void TableSchema::init(const std::vector<std::string> &cnames,
		const std::vector<column_t> &ctypes, std::string primary_column) {
	row_byte_sz_ = 0;
	primary_ = 0;
	for (int i = 0; i < cnames.size(); i++) {
		cnames_.push_back(cnames[i]);
		ctypes_.push_back(ctypes[i]);
		cpos_.push_back(row_byte_sz_);
		if (cnames[i] == primary_column) {
			primary_ = i;
		}
		row_byte_sz_ += cTypeToSize[ctypes[i]];
	}
}

int TableSchema::GetColumnNumber(std::string name) {
	for (int i = 0; i < cnames_.size(); i++) {
		if (cnames_[i] == name)
			return i;
	}
	return -1;
}

} //namespace memdb
