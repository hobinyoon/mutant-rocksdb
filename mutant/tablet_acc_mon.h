#pragma once

#include <condition_variable>
#include <set>
#include <mutex>
#include <thread>

#include "util/event_logger.h"

namespace rocksdb {

// Defined in db/memtable.h
class MemTable;
// Defined in table/block_based_table_reader.h
struct BlockBasedTable;

class TabletAccMon {
  EventLogger* _logger = NULL;

  // We don't use an expensive atomic operation here. It's about when to
  // report, and it's okay we are not super accurate about the timing. This is
  // updated everytime any tablet is read.
  bool _updatedSinceLastOutput;

  // SSTable access monitoring is for the SSTable migration decisions.
  // MemTable access monitoring is just to see/understand what's going on
  // internally.
  //
  // We keep only active memt and sst lists here. (Read) access countings are
  // done with a counter in each memt and sst.
  std::set<MemTable*> _memtSet;
  std::mutex _memtSetLock;
  std::mutex _memtSetLock2;

  std::set<BlockBasedTable*> _sstSet;
  std::mutex _sstSetLock;
  std::mutex _sstSetLock2;

  std::thread _reporter;
  std::mutex _reporter_sleep_mutex;
  std::condition_variable _reporter_sleep_cv;
  bool _reporter_wakeupnow = false;

  std::mutex _reporting_mutex;
  bool _reported = false;
  std::condition_variable _reported_cv;

  static TabletAccMon& _GetInst();

  TabletAccMon();

  void _Init(EventLogger* logger);
  void _MemtCreated(MemTable* m);
  void _MemtDeleted(MemTable* m);
  void _SstOpened(BlockBasedTable* bbt);
  void _SstClosed(BlockBasedTable* bbt);
  void _ReportAndWait();
  void _Updated();

  void _ReporterRun();
  void _ReporterSleep();
  void _ReporterWakeup();

public:
  static void Init(EventLogger* logger);
  static void MemtCreated(MemTable* m);
  static void MemtDeleted(MemTable* m);
  static void SstOpened(BlockBasedTable* bbt);
  static void SstClosed(BlockBasedTable* bbt);
  static void ReportAndWait();
  static void Updated();
};

}
