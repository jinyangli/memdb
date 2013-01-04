#include <stdio.h>
#include <time.h>
#include "memtable.h"
#include "util/testharness.h"

namespace memdb {

struct test_row {
	test_row(int f, int t, std::string fn, std::string tn) : from_id(f), to_id(t), from_name(fn), to_name(tn) {}
	test_row() {}
	int from_id;
	int to_id;
	std::string from_name;
	std::string to_name;
};

static int row_compare(const void * aptr, const void * bptr)
{
	const test_row *a = (test_row *)aptr;
	const test_row *b = (test_row *)bptr;
	if (a->from_id > b->from_id) {
		return 1;
	}else if (a->from_id < b->from_id) {
		return -1;
	}else if (a->to_id > b->to_id) {
		return 1;
	}else if (a->to_id < b->to_id) {
		return -1;
	}else{
		return 0;
	}
}


class MemdbTest {
public:
	MemdbTest() {
		std::string cnames[4] = { "from_id", "from_name", "to_id", "to_name" };
		column_t ctypes[4] = { cInt32, cString, cInt32, cString };
		schema_ = new TableSchema(4, cnames, ctypes, "to_id");
		table_ = new MemTable(schema_);
	}

	TableSchema *schema_;
	MemTable *table_;
};

TEST(MemdbTest, SortedSmall) {
	const int N = 4;
	test_row allrows[4];

	allrows[0] = test_row(1,2,"crap","junk");
	allrows[1] = test_row(1,3,"crap","junk");
	allrows[2] = test_row(3,1,"crap","junk");
	allrows[3] = test_row(2,3,"crap","junk");

	qsort(allrows, N, sizeof(test_row), row_compare);
	printf("from_id\tfrom_name\tto_id\t to_name\n");
	for (int i = 0; i < N; i++) {
		printf("%d\t%s\t%d\t%s\n", allrows[i].from_id, allrows[i].from_name.c_str(),\
				allrows[i].to_id,allrows[i].to_name.c_str());
	}

	table_->Clear();
	for (int i = N-1; i >= 0; i--) {
		Row r(schema_);
		r << allrows[i].from_id << allrows[i].from_name << allrows[i].to_id << allrows[i].to_name;
		table_->InsertRow(r);
	}
	table_->PrintAll();
}

	/*
TEST(MemdbTest, SortedBig) {
	const int N = 10000;
	test_row allrows[N];
	for (int i = 0; i < N; i++) {
		allrows[i].from_id = random() % 10000;
		allrows[i].to_id = random() % 10000;
		allrows[i].from_name = test::RandomStr(20);
		allrows[i].to_name = test::RandomStr(20);
	}
	qsort();
	table_->Clear();
		
}
	*/

TEST(MemdbTest, InsertSpeed) {

	table_->Clear();
	const int N = 1000000;
	std::string rs1 = "random dude1";
	std::string rs2 = "random dude2";

	struct timespec start, end;
	clock_gettime(CLOCK_REALTIME, &start);
	for (int i = 0; i < N; i++) {
		int from = random() % 1000000;
		int to = random() % 1000000;
		Row r(schema_);
		r << from << rs1 << to << rs2;
		table_->InsertRow(r);
	}
	clock_gettime(CLOCK_REALTIME, &end);
	printf("inserting %d rows in %lu usec total\n", N,
			test::timediff(&end, &start));
}

} //namespace memdb

int main(int argc, char** argv) {
	return memdb::test::RunAllTests();
}

