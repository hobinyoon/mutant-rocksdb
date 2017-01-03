#include <atomic>
#include <boost/algorithm/string/join.hpp>
#include <boost/format.hpp>

#include "db/event_helpers.h"
#include "db/version_edit.h"
#include "mutant/tablet_acc_mon.h"
#include "table/block_based_table_reader.h"
#include "util/util.h"

using namespace std;

namespace rocksdb {

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


void TabletAccMon::_MemtCreated(MemTable* m) {
  lock_guard<mutex> lk(_memtSetLock);

  //TRACE << boost::format("MemtCreated %p\n") % m;

  auto it = _memtSet.find(m);
  if (it == _memtSet.end()) {
    lock_guard<mutex> lk2(_memtSetLock2);
    _memtSet.insert(m);
    // Note: Log creation/deletion time, if needed. You don't need to wake up
    // the reporter thread.
  } else {
    THROW("Unexpected");
  }
}


void TabletAccMon::_MemtDeleted(MemTable* m) {
  lock_guard<mutex> lk(_memtSetLock);

  //TRACE << boost::format("MemtDeleted %p\n") % m;

  auto it = _memtSet.find(m);
  if (it == _memtSet.end()) {
    THROW("Unexpected");
  } else {
    // Report for the last time before erasing the entry
    _ReportAndWait();

    lock_guard<mutex> lk2(_memtSetLock2);
    _memtSet.erase(it);
  }
}


// FileDescriptor constructor/destructor is not good ones to monitor. They are
// copyable and the same address is opened and closed multiple times.
void TabletAccMon::_SstOpened(BlockBasedTable* bbt) {
  lock_guard<mutex> lk(_sstSetLock);

  //TRACE << boost::format("SstOpened %d\n") % bbt->SstId();

  auto it = _sstSet.find(bbt);
  if (it == _sstSet.end()) {
    lock_guard<mutex> lk2(_sstSetLock2);
    _sstSet.insert(bbt);
  } else {
    THROW("Unexpected");
  }
}


void TabletAccMon::_SstClosed(BlockBasedTable* bbt) {
  lock_guard<mutex> lk(_sstSetLock);

  //TRACE << boost::format("SstClosed %d\n") % bbt->SstId();

  auto it = _sstSet.find(bbt);
  if (it == _sstSet.end()) {
    THROW("Unexpected");
  } else {
    // Report for the last time before erasing the entry.
    _ReportAndWait();

    // You need a 2-level locking with _sstSetLock and _sstSetLock2.
    // _ReportAndWait() waits for the reporter thread to finish a cycle, which
    // acquires _sstSetLock2. The same goes with _memtSetLock2.
    lock_guard<mutex> lk2(_sstSetLock2);
    _sstSet.erase(it);
  }
}


void TabletAccMon::_Updated() {
  _updatedSinceLastOutput = true;
}


void TabletAccMon::_ReporterRun() {
  try {
    while (true) {
      // Sleep for reportIntervalMs or until woken up by another thread.
      {
        static const long reportIntervalMs = 1000;
        static const auto wait_dur = chrono::milliseconds(reportIntervalMs);
        unique_lock<mutex> lk(_reporter_sleep_mutex);
        _reporter_sleep_cv.wait_for(lk, wait_dur, [&](){return _reporter_wakeupnow;});
        _reporter_wakeupnow = false;
      }

      {
        lock_guard<mutex> lk(_reporting_mutex);

        // A 2-level check. _reporter_sleep_cv alone is not enough, since this
        // thread is woken up every second anyway.
        //
        // Print out the access stat. The entries are already sorted numerically.
        if (_updatedSinceLastOutput) {
          // Output to the rocksdb log
          JSONWriter jwriter;
          EventHelpers::AppendCurrentTime(&jwriter);
          jwriter << "mutant_table_acc_cnt";
          {
            jwriter.StartObject();
            {
              vector<string> vs;
              {
                lock_guard<mutex> lk2(_memtSetLock2);
                for (auto mt: _memtSet) {
                  // Get-and-reset the read counter
                  long c = mt->_num_reads.exchange(0);
                  if (c > 0) {
                    // This doesn't work. I guess only constant strings can be keys
                    //jwriter << mt.first << c;
                    vs.push_back(str(boost::format("%p:%d") % mt % c));
                  }
                }
              }
              if (vs.size() > 0)
                jwriter << "memt" << boost::algorithm::join(vs, " ");
            }
            {
              vector<string> vs;
              {
                lock_guard<mutex> lk2(_sstSetLock2);
                for (auto bbt: _sstSet) {
                  // Get-and-reset the read counter
                  long c = bbt->GetAndResetNumReads();
                  if (c > 0)
                    vs.push_back(str(boost::format("%d:%d") % bbt->SstId() % c));
                }
              }
              if (vs.size() > 0)
                jwriter << "sst" << boost::algorithm::join(vs, " ");
            }
            jwriter.EndObject();
          }
          jwriter.EndObject();
          _logger->Log(jwriter);
          _updatedSinceLastOutput = false;
        }
        _reported = true;
      }
      _reported_cv.notify_one();
    }
  } catch (const exception& e) {
    TRACE << boost::format("Exception: %s\n") % e.what();
    exit(0);
  }
}


void TabletAccMon::_ReporterWakeup() {
  // This wakes up the waiting thread even if this is called before wait.
  // _reporter_wakeupnow does the magic.
  {
    lock_guard<mutex> lk(_reporter_sleep_mutex);
    _reporter_wakeupnow = true;
  }
  _reporter_sleep_cv.notify_one();
}


void TabletAccMon::_ReportAndWait() {
  // The whole reporting block needs to be protected for this.  When the
  // reporter thread is ready entering the reporting block, the reporting block
  // can be called twice, which is okay - the second one won't print out
  // anything.
  {
    lock_guard<mutex> _(_reporting_mutex);
    _reported = false;
  }

  //TRACE << "force reporting\n";
  _ReporterWakeup();

  // Wait for the reporter finish a reporting
  {
    unique_lock<mutex> lk(_reporting_mutex);
    _reported_cv.wait(lk, [&](){return _reported;});
  }
}


void TabletAccMon::Init(EventLogger* logger) {
  static TabletAccMon& i = _GetInst();
  i._Init(logger);
}


void TabletAccMon::MemtCreated(MemTable* m) {
  static TabletAccMon& i = _GetInst();
  i._MemtCreated(m);
}


void TabletAccMon::MemtDeleted(MemTable* m) {
  static TabletAccMon& i = _GetInst();
  i._MemtDeleted(m);
}


void TabletAccMon::SstOpened(BlockBasedTable* bbt) {
  static TabletAccMon& i = _GetInst();
  i._SstOpened(bbt);
}


void TabletAccMon::SstClosed(BlockBasedTable* bbt) {
  static TabletAccMon& i = _GetInst();
  i._SstClosed(bbt);
}


void TabletAccMon::ReportAndWait() {
  static TabletAccMon& i = _GetInst();
  i._ReportAndWait();
}


void TabletAccMon::Updated() {
  static TabletAccMon& i = _GetInst();
  i._Updated();
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
//         _memtSet.clear();
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
//         if (_memtSet.get(m) == null)
//             _memtSet.put(m, new _AccCnt(0));
//         _or.Wakeup();
//     }
//
//     // MemTable discarded
//     public static void Discarded(Memtable m) {
//         _AccCnt v = _memtSet.get(m);
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
