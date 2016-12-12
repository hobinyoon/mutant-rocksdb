#include <csignal>
#include <cstdio>
#include <memory>
#include <string>

#include <boost/format.hpp>
#include <boost/regex.hpp>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "conf.h"
#include "cons.h"
#include "util.h"

using namespace std;
using namespace rocksdb;


void on_signal(int sig) {
	cout << boost::format("\nGot a signal: %d\n%s\n") % sig % Util::Indent(Util::StackTrace(1), 2);
  exit(1);
}


string _db_path;
DB* _db;


// Optimize later
string GenRandomAsciiString(const int len) {
	vector<char> s(len);
	static const char alphanum[] =
		"0123456789"
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		"abcdefghijklmnopqrstuvwxyz";

	for (int i = 0; i < len; ++i) {
		s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
	}

	return string(s.begin(), s.end());
}


void Insert(const int num_keys) {
	Cons::MT _(boost::format("Inserting %d key-value pairs ...") % num_keys);
	for (int i = 0; i < num_keys; i ++) {
		string k = str(boost::format("%010d") % i);
		string v = GenRandomAsciiString(990);

		static const auto wo = WriteOptions();
		Status s = _db->Put(wo, k, v);
		if (! s.ok())
			THROW("Put failed");
	}
}


int main(int argc, char* argv[]) {
	try {
		signal(SIGSEGV, on_signal);
		signal(SIGINT, on_signal);

		Conf::Init(argc, argv);
		_db_path = boost::regex_replace(Conf::GetStr("db_path"), boost::regex("~"), Util::HomeDir());
		Cons::P(boost::format("_db_path=%s") % _db_path);

		Options options;
		// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
		options.IncreaseParallelism();
		options.OptimizeLevelStyleCompaction();
		// create the DB if it's not already present
		options.create_if_missing = true;

		// Open DB
		Status s = DB::Open(options, _db_path, &_db);
		if (! s.ok())
			THROW("DB::Open failed");

		Insert(10000);

		// Get value
		//string value;
		//s = _db->Get(ReadOptions(), "key1", &value);
		//if (! s.ok())
		//	THROW("Get failed");
		//if (value != "value")
		//	THROW("Unexpected");

		delete _db;
	} catch (const exception& e) {
		cerr << "Got an exception: " << e.what() << "\n";
		return 1;
	}
	return 0;
}


// Atomically apply a set of updates
// Note: might be useful for record reinsertions
// {
//   WriteBatch batch;
//   batch.Delete("key1");
//   batch.Put("key2", value);
//   s = db->Write(WriteOptions(), &batch);
// }
