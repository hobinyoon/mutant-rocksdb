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


void Insert(int lower, int upper) {
	Cons::MT _(boost::format("Inserting key-value pairs in range [%d, %d) ...")
      % lower % upper);
	for (int i = lower; i < upper; i += 2) {
		string k = str(boost::format("%010d") % i);
		string v = GenRandomAsciiString(990);

		static const auto wo = WriteOptions();
		Status s = _db->Put(wo, k, v);
		if (! s.ok())
			THROW("Put failed");
	}
}


void Get(const string& k) {
	string v;
	static const auto ro = ReadOptions();
	Status s = _db->Get(ro, k, &v);
  if (s.IsNotFound()) {
    //Cons::P(boost::format("Key %s not found") % k);
    return;
  }
	if (! s.ok())
		THROW("Get failed");
  //Cons::P(boost::format("Value=%s") % v);
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

		//Insert(    0, 20000);
    for (int i = 0; i < 100; i ++) {
      Get("0000010001");
    }
    sleep(1.5);

    //Insert(0, 1);
    Get("0000000000");

    //Get("0");
    Get("A");

    // Give some time for TabletAccMon to print stuff
    sleep(5);

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
