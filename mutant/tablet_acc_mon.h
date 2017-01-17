#pragma once

#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <thread>

#include <boost/date_time/posix_time/posix_time.hpp>


namespace rocksdb {

struct BlockBasedTable;
class ColumnFamilyData;
class DBImpl;
class EventLogger;
struct FileDescriptor;
struct FileMetaData;
class MemTable;
class TableReader;

class SstTemp;

class TabletAccMon {
  DBImpl* _db = nullptr;
  EventLogger* _logger = nullptr;
  ColumnFamilyData* _cfd = nullptr;

  // This is updated very frequently by threads whenever a SSTable or a
  // MemTable is read, thus we don't used an expensive atomic operation here.
  //
  // It only affects the temp updater thread when to update, and it's ok that
  // it's delayed by a tiny little bit, due to the relaxed cache coherency.
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

  // map<sst_id, SstTemp*>
  std::map<uint64_t, SstTemp*> _sstMap;
  std::mutex _sstMapLock;
  std::mutex _sstMapLock2;

  std::thread _temp_updater_thread;
  std::mutex _temp_updater_sleep_mutex;
  std::condition_variable _temp_updater_sleep_cv;
  bool _temp_updater_wakeupnow = false;

  std::mutex _temp_updating_mutex;
  bool _temp_updated = false;
  std::condition_variable _temp_updated_cv;

  bool _temp_updater_stop_requested = false;

  // SSTable migration triggerer
  std::thread _smt_thread;
  std::mutex _smt_sleep_mutex;
  std::condition_variable _smt_sleep_cv;
  bool _smt_wakeupnow = false;

  bool _smt_stop_requested = false;

  static TabletAccMon& _GetInst();

  TabletAccMon();

  void _Init(DBImpl* db, EventLogger* el);
  void _MemtCreated(ColumnFamilyData* cfd, MemTable* m);
  void _MemtDeleted(MemTable* m);
  void _SstOpened(TableReader* tr, const FileDescriptor* fd, int level);
  void _SstClosed(BlockBasedTable* bbt);
  void _RunTempUpdaterAndWait();
  void _SetUpdated();
  double _Temperature(uint64_t sst_id, const boost::posix_time::ptime& cur_time);
  uint32_t _CalcOutputPathId(
      bool temperature_triggered_single_sstable_compaction,
      const std::vector<FileMetaData*>& file_metadata);
  uint32_t _CalcOutputPathIdTrivialMove(const FileMetaData* fmd);
  FileMetaData*_PickSstForMigration(int& level_for_migration);

  void _TempUpdaterRun();
  void _TempUpdaterSleep();
  void _TempUpdaterWakeup();

  void _SstMigrationTriggererRun();
  void _SstMigrationTriggererSleep();
  void _SstMigrationTriggererWakeup();

  void _Shutdown();

public:
  static void Init(DBImpl* db, EventLogger* el);
  static void MemtCreated(ColumnFamilyData* cfd, MemTable* m);
  static void MemtDeleted(MemTable* m);
  static void SstOpened(TableReader* tr, const FileDescriptor* fd, int level);
  static void SstClosed(BlockBasedTable* bbt);
  static void SetUpdated();

  static uint32_t CalcOutputPathId(
      bool temperature_triggered_single_sstable_compaction,
      const std::vector<FileMetaData*>& file_metadata);
  static uint32_t CalcOutputPathIdTrivialMove(const FileMetaData* fmd);

  static FileMetaData* PickSstForMigration(int& level_for_migration);

  static void Shutdown();
};

}
