#include <boost/algorithm/string/join.hpp>
#include <boost/format.hpp>

#include "db/event_helpers.h"
#include "mutant/tablet_acc_mon.h"
#include "util/util.h"

using namespace std;

namespace rocksdb {

struct _AccCnt {
  // The number of accesses to the MemTable. The requested record may or may
  // not be there.
  std::atomic<long> cnt;
  bool discarded = false;
  // Set to true right before getting logged for the last time
  bool loggedAfterDiscarded = false;

  _AccCnt(long cnt_)
    : cnt(cnt_)
  { }

  void Increment() {
    cnt ++;
  }

  long GetAndReset() {
    return cnt.exchange(0);
  }
};


TabletAccMon& TabletAccMon::_GetInst() {
  static TabletAccMon i;
  return i;
}


// This object is not released, since it's a singleton object.
TabletAccMon::TabletAccMon()
: _updatedSinceLastOutput(false)
  , _reporter(thread(bind(&rocksdb::TabletAccMon::_ReporterRun, this)))
{
  // http://stackoverflow.com/questions/7381757/c-terminate-called-without-an-active-exception
  _reporter.detach();
}


void TabletAccMon::_Init(EventLogger* logger) {
  _logger = logger;
  //TRACE << "TabletAccMon initialized\n";

  JSONWriter jwriter;
  EventHelpers::AppendCurrentTime(&jwriter);
  jwriter << "mutant_table_acc_mon_init";
  jwriter.EndObject();
  _logger->Log(jwriter);
}


void TabletAccMon::_MemtRead(void* m) {
  _updatedSinceLastOutput = true;

  // Test and test-and-set style
  auto it = _memtAccCnt.find(m);
  if (it == _memtAccCnt.end()) {
    lock_guard<mutex> _(_memtAccCntLock);

    auto it2 = _memtAccCnt.find(m);
    if (it2 == _memtAccCnt.end()) {
      _memtAccCnt[m] = new _AccCnt(1);
      // Force reporting when a new Memtable is created. This may be helpful
      // for logging the exact time of creations/deletions when Memtable
      // creation/deletion events are not logged.
      //
      // Better log the creation/deletion events explicitly.
      //_ReporterWakeup();
    } else {
      (it2->second)->Increment();
    }
  } else {
    (it->second)->Increment();
  }

  //TRACE << boost::format("Mutant: Memtable %p accessed\n") % m;
}


// I wonder how much overhead this Monitoring has.  Might be a good one to
// present.
void TabletAccMon::_SstRead(uint64_t s) {
  _updatedSinceLastOutput = true;

  // Test and test-and-set style. The longest path is taken only in the
  // beginning when _sstAccCnt doesn't have s.
  auto it = _sstAccCnt.find(s);
  if (it == _sstAccCnt.end()) {
    lock_guard<mutex> _(_sstAccCntLock);

    auto it2 = _sstAccCnt.find(s);
    if (it2 == _sstAccCnt.end()) {
      _sstAccCnt[s] = new _AccCnt(1);
      // Force reporting
      //
      // TODO: Not sure if you need this. SSTable creation event is already
      // logged.
      //_ReporterWakeup();
    } else {
      (it2->second)->Increment();
    }
  } else {
    (it->second)->Increment();
  }

  //TRACE << boost::format("Mutant: Read SSTable %d\n") % s;
}


void TabletAccMon::_ReporterRun() {
  try {
    while (true) {
      _ReporterSleep();

      // A 2-level check. _reporter_sleep_cv alone is not enough.
      if (! _updatedSinceLastOutput.exchange(false))
        continue;

      // Remove discarded MemTables and SSTables after logging for the last time
      for (auto i: _memtAccCnt) {
        if (i.second->discarded)
          i.second->loggedAfterDiscarded = true;
      }
      for (auto i: _sstAccCnt) {
        if (i.second->discarded)
          i.second->loggedAfterDiscarded = true;
      }

      // Print out the access stat. The entries are already sorted numerically.
      {
        if (true) {
          // Output to the rocksdb log
          JSONWriter jwriter;
          EventHelpers::AppendCurrentTime(&jwriter);
          jwriter << "mutant_table_acc_cnt";
          {
            jwriter.StartObject();
            jwriter << "memt";
            {
              vector<string> vs;
              for (auto i: _memtAccCnt) {
                long c = i.second->GetAndReset();
                if (c > 0) {
                  // This doesn't work. I guess only constant strings can be keys
                  //jwriter << i.first << c;
                  vs.push_back(str(boost::format("%p:%d") % i.first % c));
                }
              }
              jwriter << boost::algorithm::join(vs, " ");
            }
            jwriter << "sst";
            {
              vector<string> vs;
              for (auto i: _sstAccCnt) {
                long c = i.second->GetAndReset();
                if (c > 0)
                  vs.push_back(str(boost::format("%p:%d") % i.first % c));
              }
              jwriter << boost::algorithm::join(vs, " ");
            }
            jwriter.EndObject();
          }
          _logger->Log(jwriter);
        } else {
          // Output to the client console
          vector<string> outEntries;
          for (auto i: _memtAccCnt) {
            long c = i.second->GetAndReset();
            if (c > 0)
              outEntries.push_back(str(boost::format("m%s:%s") % i.first % c));
          }
          for (auto i: _sstAccCnt) {
            long c = i.second->GetAndReset();
            if (c > 0)
              outEntries.push_back(str(boost::format("s%d:%s") % i.first % c));
          }
          if (outEntries.size() > 0)
            TRACE << boost::format("Mutant: TabletAccCnt: %s\n") % boost::algorithm::join(outEntries, " ");
        }
      }

      // Remove Memtables and SSTables that are discarded and written to logs
      for (auto it = _memtAccCnt.cbegin(); it != _memtAccCnt.cend(); ) {
        if (it->second->loggedAfterDiscarded) {
          it = _memtAccCnt.erase(it);
        } else {
          it ++;
        }
      }
      for (auto it = _sstAccCnt.cbegin(); it != _sstAccCnt.cend(); ) {
        if (it->second->loggedAfterDiscarded) {
          it = _sstAccCnt.erase(it);
        } else {
          it ++;
        }
      }
    }
  } catch (const exception& e) {
    TRACE << boost::format("Exception: %s\n") % e.what();
    exit(0);
  }
}


static const long reportIntervalMs = 1000;

void TabletAccMon::_ReporterSleep() {
  // http://en.cppreference.com/w/cpp/thread/condition_variable/wait_for
  std::unique_lock<std::mutex> lk(_reporter_sleep_mutex);
  static const auto wait_dur = std::chrono::milliseconds(reportIntervalMs);
  _reporter_sleep_cv.wait_for(lk, wait_dur, [&](){return _reporter_wakeupnow;});
  _reporter_wakeupnow = false;
}


void TabletAccMon::_ReporterWakeup() {
  {
    std::unique_lock<std::mutex> lk(_reporter_sleep_mutex);
    _reporter_wakeupnow = true;
  }
  _reporter_sleep_cv.notify_one();
}


void TabletAccMon::Init(EventLogger* logger) {
  static TabletAccMon& i = _GetInst();
  i._Init(logger);
}


void TabletAccMon::MemtRead(void* m) {
  static TabletAccMon& i = _GetInst();
  i._MemtRead(m);
}


void TabletAccMon::SstRead(uint64_t s) {
  static TabletAccMon& i = _GetInst();
  i._SstRead(s);
}

}


// package org.apache.cassandra.mutant;
//
// import java.sql.Timestamp;
// import java.text.SimpleDateFormat;
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.Comparator;
// import java.util.Map;
// import java.util.concurrent.atomic.AtomicLong;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.Iterator;
// import java.util.List;
//
// import org.apache.cassandra.config.Config;
// import org.apache.cassandra.config.DatabaseDescriptor;
// import org.apache.cassandra.db.Memtable;
// import org.apache.cassandra.io.sstable.Descriptor;
// import org.apache.cassandra.io.sstable.format.SSTableReader;
//
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// public class MemSsTableAccessMon
// {
//     // Called when a ColumnFamilyStore (table) is created.
//     public static void Reset() {
//         _memtAccCnt.clear();
//         _ssTableAccCnt.clear();
//         logger.warn("Mutant: ResetMon");
//         logger.warn("Mutant: Node configuration:[{}]", Config.GetNodeConfigStr());
//     }
//
//     //public static void BloomfilterPositive(SSTableReader r) {
//     //    Descriptor sst_desc = r.descriptor;
//     //
//     //    _SSTableAccCnt sstAC = _ssTableAccCnt.get(sst_desc);
//     //    if (sstAC == null) {
//     //        sstAC = new _SSTableAccCnt(r);
//     //        sstAC.IncrementBfPositives();
//     //        _ssTableAccCnt.put(sst_desc, sstAC);
//     //        _updatedSinceLastOutput = true;
//     //        _or.Wakeup();
//     //    } else {
//     //        sstAC.IncrementBfPositives();
//     //        _updatedSinceLastOutput = true;
//     //    }
//     //}
//
//     // MemTable created
//     public static void Created(Memtable m) {
//         logger.warn("Mutant: MemtCreated {}", m);
//         if (_memtAccCnt.get(m) == null)
//             _memtAccCnt.put(m, new _AccCnt(0));
//         _or.Wakeup();
//     }
//
//     // MemTable discarded
//     public static void Discarded(Memtable m) {
//         _AccCnt v = _memtAccCnt.get(m);
//         if (v == null) {
//             // Can a memtable be discarded without being accessed at all? I'm
//             // not sure, but let's not throw an exception.
//             return;
//         }
//         v.discarded = true;
//
//         _updatedSinceLastOutput = true;
//         logger.warn("Mutant: MemtDiscard {}", m);
//         _or.Wakeup();
//     }
//
//     // SSTable discarded
//     public static void Deleted(Descriptor d) {
//         _SSTableAccCnt v = _ssTableAccCnt.get(d);
//         if (v == null) {
//             // A SSTable can be deleted without having been accessed by
//             // starting Cassandra, dropping an existing keyspace.
//             return;
//         }
//         v.deleted = true;
//
//         _updatedSinceLastOutput = true;
//         logger.warn("Mutant: SstDeleted {}", d);
//         _or.Wakeup();
//     }
// }
