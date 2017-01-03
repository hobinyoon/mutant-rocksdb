#pragma once

#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <thread>

#include "util/event_logger.h"

namespace rocksdb {

// Defined in db/memtable.h
class MemTable;
// Defined in table/block_based_table_reader.h
struct BlockBasedTable;

class SstMeta;

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
  //
  // Note: disable MemTable monitoring when measuring the monitoring overhead.
  std::set<MemTable*> _memtSet;
  std::mutex _memtSetLock;
  std::mutex _memtSetLock2;

  std::map<BlockBasedTable*, SstMeta*> _sstMap;
  std::mutex _sstMapLock;
  std::mutex _sstMapLock2;

  std::thread _reporter_thread;
  std::mutex _reporter_sleep_mutex;
  std::condition_variable _reporter_sleep_cv;
  bool _reporter_wakeupnow = false;

  std::mutex _reporting_mutex;
  bool _reported = false;
  std::condition_variable _reported_cv;

  // SSTable migration triggerer
  std::thread _smt_thread;
  std::mutex _smt_sleep_mutex;
  std::condition_variable _smt_sleep_cv;
  bool _smt_wakeupnow = false;

  static TabletAccMon& _GetInst();

  TabletAccMon();

  void _Init(EventLogger* logger);
  void _MemtCreated(MemTable* m);
  void _MemtDeleted(MemTable* m);
  void _SstOpened(BlockBasedTable* bbt, uint64_t size);
  void _SstClosed(BlockBasedTable* bbt);
  void _ReportAndWait();
  void _Updated();

  void _ReporterRun();
  void _ReporterSleep();
  void _ReporterWakeup();

  void _SstMigrationTriggererRun();
  void _SstMigrationTriggererSleep();
  void _SstMigrationTriggererWakeup();

public:
  static void Init(EventLogger* logger);
  static void MemtCreated(MemTable* m);
  static void MemtDeleted(MemTable* m);
  static void SstOpened(BlockBasedTable* bbt, uint64_t size);
  static void SstClosed(BlockBasedTable* bbt);
  static void ReportAndWait();
  static void Updated();
};

}
