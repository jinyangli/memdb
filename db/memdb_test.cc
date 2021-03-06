#include <stdio.h>
#include <time.h>
#include <assert.h>
#include "memtable.h"
#include "util/testharness.h"

namespace memdb {

struct test_row {
	test_row(int f, int t, std::string fn, std::string tn) :
			from_id(f), to_id(t) {
		from_name = new std::string(fn);
		to_name = new std::string(tn);
	}
	test_row() {
	}
	int from_id;
	int to_id;
	std::string *from_name;
	std::string *to_name;
};

static int row_compare(const void * aptr, const void * bptr) {
	const test_row *a = (test_row *) aptr;
	const test_row *b = (test_row *) bptr;
	if (a->from_id > b->from_id) {
		return 1;
	} else if (a->from_id < b->from_id) {
		return -1;
	} else if (a->to_id > b->to_id) {
		return 1;
	} else if (a->to_id < b->to_id) {
		return -1;
	} else {
		return 0;
	}
}

static int bsearch(test_row *rows, int start, int end, int from_id, int to_id) {
	if (start == end) {
		if ((rows[start].from_id < from_id)
				|| ((rows[start].from_id == from_id)
						&& (rows[start].to_id < to_id))) {
			return start + 1;
		} else
			return start;
	}

	int mid = start + ((end - start) / 2);
	if ((rows[mid].from_id < from_id)
			|| ((rows[mid].from_id == from_id) && (rows[mid].to_id < to_id))) {
		return bsearch(rows, mid + 1, end, from_id, to_id);
	} else
		return bsearch(rows, start, mid, from_id, to_id);
}

class MemdbTest {
public:
	MemdbTest() {
		std::string cnames[4] = { "from_id", "from_name", "to_id", "to_name" };
		column_t ctypes[4] = { cInt32, cString, cInt32, cString };
		schema_ = new TableSchema(4, cnames, ctypes, "to_id");
		table_ = new MemTable(schema_);
		allrows_ = NULL;
	}

	//create n distinct rows
	void InitTestRows(int n) {
		if (allrows_)
			free(allrows_);
		allrows_ = (test_row *) malloc(sizeof(test_row) * n);
		assert(allrows_);

		std::map<long, int> existing;
		int i = 0;
		while (i < n) {
			allrows_[i] = test_row(random() % 1000000, random() % 1000000,
					test::RandomStr(20), test::RandomStr(20));
			long k = allrows_[i].from_id;
			k = k << 32 | allrows_[i].to_id;
			if (existing.find(k) == existing.end()) {
				existing[k] = 1;
				i++;
			}
		}
		printf("initialized %d testing rows\n", n);
	}

	void DumpToTable(int n) {
		table_->Clear();
		for (int i = 0; i < n; i++) {
			RwRow r(table_);
			r << allrows_[i].from_id << *(allrows_[i].from_name)
					<< allrows_[i].to_id << *(allrows_[i].to_name);
			table_->InsertRow(r);
		}
	}

	test_row *allrows_;

	TableSchema *schema_;
	MemTable *table_;
};

TEST(MemdbTest, SortedSmall) {
	const int N = 5;
	allrows_ = (test_row *) malloc(sizeof(test_row) * 5);
	allrows_[0] = test_row(1, 2, "alice", "bob");
	allrows_[1] = test_row(1, 3, "alice", "eve");
	allrows_[2] = test_row(2, 1, "bob", "alice");
	allrows_[3] = test_row(2, 3, "bob", "eve");
	allrows_[4] = test_row(3, 1, "eve", "alice");
	DumpToTable(N);

	//print sorted test_rows
	qsort(allrows_, N, sizeof(test_row), row_compare);
	printf("from_id\tfrom_name\tto_id\t to_name\n");
	for (int i = 0; i < N; i++) {
		printf("%d\t%s\t%d\t%s\n", allrows_[i].from_id,
				allrows_[i].from_name->c_str(), allrows_[i].to_id,
				allrows_[i].to_name->c_str());
	}
	//print sorted table
	table_->PrintAll();
}

TEST(MemdbTest, SortedBig) {
	const int N = 1000000;
	InitTestRows(N);
	DumpToTable(N);

	qsort(allrows_, N, sizeof(test_row), row_compare);
	int i = 0;
	MemTable::Iterator it = MemTable::Iterator(table_);
	RdOnlyRow r(table_);
	for (it.SeekToFirst(); it.Valid(); it.Next()) {
		r = it.RowAt(r);
		ASSERT_EQ(allrows_[i].from_id, r.GetIntColumn(0));
		ASSERT_EQ(allrows_[i].to_id, r.GetIntColumn(2));
		ASSERT_EQ(*(allrows_[i].from_name), r.GetStrColumn(1));
		ASSERT_EQ(*(allrows_[i].to_name), r.GetStrColumn(3));
		i++;
	}
	printf("MemTable sorted %d rows correctly\n", N);
}

TEST(MemdbTest, InsertSpeed) {
	const int N = 1000000;
	InitTestRows(N);
	struct timespec start, end;
	clock_gettime(CLOCK_REALTIME, &start);
	DumpToTable(N);
	clock_gettime(CLOCK_REALTIME, &end);
	printf("inserting %d rows,  %lu usec per row\n", N,
			test::timediff(&end, &start) / N);
}

TEST(MemdbTest, QuerySmall) {
	const int N = 5;
	allrows_ = (test_row *) malloc(sizeof(test_row) * 5);
	allrows_[0] = test_row(1, 2, "alice", "bob");
	allrows_[1] = test_row(1, 3, "alice", "eve");
	allrows_[2] = test_row(2, 1, "bob", "alice");
	allrows_[3] = test_row(2, 3, "bob", "eve");
	allrows_[4] = test_row(3, 1, "eve", "alice");
	DumpToTable(N);
	qsort(allrows_, N, sizeof(test_row), row_compare);

	MemTable::Iterator it = MemTable::Iterator(table_);
	RdOnlyRow r(table_);
	it.Seek(2);
	assert(it.Valid(2));
	r = it.RowAt(r);
	int ri = bsearch(allrows_, 0, N - 1, 2, 0);
	ASSERT_EQ(r.GetIntColumn(0), allrows_[ri].from_id);
	ASSERT_EQ(r.GetIntColumn(2), allrows_[ri].to_id);

	it.Next();
	assert(it.Valid(2));
	r = it.RowAt(r);
	ASSERT_EQ(r.GetIntColumn(0), allrows_[ri+1].from_id);
	ASSERT_EQ(r.GetIntColumn(2), allrows_[ri+1].to_id);

	it.Seek(1, 3);
	r = it.RowAt(r);
	assert(it.Valid(1, 3));
	ri = bsearch(allrows_, 0, N - 1, 1, 3);
	ASSERT_EQ(r.GetIntColumn(0), allrows_[ri].from_id);
	ASSERT_EQ(r.GetIntColumn(2), allrows_[ri].to_id);
	it.Next();
	assert(!it.Valid(1, 3));

	printf("querying a small table correctly\n");
}

TEST(MemdbTest, QueryBig) {
	const int N = 1000000;
	InitTestRows(N);
	DumpToTable(N);

	for (int i = 0; i < 5; i++) {
		int from = allrows_[random() % N].from_id;
		qsort(allrows_, N, sizeof(test_row), row_compare);
		int ri = bsearch(allrows_, 0, N - 1, from, 0);
		RdOnlyRow r(table_);
		MemTable::Iterator it = MemTable::Iterator(table_);
		it.Seek(from);
		while (allrows_[ri].from_id == from) {
			r = it.RowAt(r);
			ASSERT_EQ(r.GetIntColumn(0), allrows_[ri].from_id);
			ASSERT_EQ(r.GetIntColumn(2), allrows_[ri].to_id);
			it.Next();
			ri++;
		}
	}

	const int NUM_QUERIES = 1000;
	struct timespec start, end;
	clock_gettime(CLOCK_REALTIME, &start);
	for (int i = 0; i < NUM_QUERIES; i++) {
		int x = random() % N;
		MemTable::Iterator it = MemTable::Iterator(table_);
		it.Seek(allrows_[x].from_id, allrows_[x].to_id);
		RdOnlyRow r(table_);
		r = it.RowAt(r);
		ASSERT_EQ(r.GetIntColumn(0), allrows_[x].from_id);
		ASSERT_EQ(r.GetIntColumn(2), allrows_[x].to_id);

	}
	clock_gettime(CLOCK_REALTIME, &end);
	printf("%lu usec per query\n", test::timediff(&end, &start) / NUM_QUERIES);
}

} //namespace memdb

int main(int argc, char** argv) {
	return memdb::test::RunAllTests();
}

