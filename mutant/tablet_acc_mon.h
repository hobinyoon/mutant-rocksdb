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

  // map<sst_id, SstTemp*>
  std::map<uint64_t, SstTemp*> _sstMap;
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

  void _Init(DBImpl* db, EventLogger* el);
  void _MemtCreated(ColumnFamilyData* cfd, MemTable* m);
  void _MemtDeleted(MemTable* m);
  void _SstOpened(TableReader* tr, const FileDescriptor* fd, int level);
  // TODO: I don't like this. Doesn't feel like a clean interface. Once it's
  // working, rewrite it. Call SstOpened() at 3 separate places.
  void _SstSetLevel(uint64_t sst_id, int level);
  void _SstClosed(BlockBasedTable* bbt);
  void _ReportAndWait();
  void _SetUpdated();
  double _Temperature(uint64_t sst_id, const boost::posix_time::ptime& cur_time);
  uint32_t _CalcOutputPathId(
      bool temperature_triggered_single_sstable_compaction,
      const std::vector<FileMetaData*>& file_metadata,
      std::vector<std::string>& input_sst_info);
  uint32_t _CalcOutputPathId(const FileMetaData* fmd);
  FileMetaData*_PickSstForMigration(int& level_for_migration);

  void _ReporterRun();
  void _ReporterSleep();
  void _ReporterWakeup();

  void _SstMigrationTriggererRun();
  void _SstMigrationTriggererSleep();
  void _SstMigrationTriggererWakeup();

public:
  static void Init(DBImpl* db, EventLogger* el);
  static void MemtCreated(ColumnFamilyData* cfd, MemTable* m);
  static void MemtDeleted(MemTable* m);
  static void SstOpened(TableReader* tr, const FileDescriptor* fd, int level);
  static void SstSetLevel(uint64_t sst_id, int level);
  static void SstClosed(BlockBasedTable* bbt);
  static void ReportAndWait();
  static void SetUpdated();

  static uint32_t CalcOutputPathId(
      bool temperature_triggered_single_sstable_compaction,
      const std::vector<FileMetaData*>& file_metadata,
      std::vector<std::string>& input_sst_info);
  static uint32_t CalcOutputPathId(const FileMetaData* fmd);

  static FileMetaData* PickSstForMigration(int& level_for_migration);
};

}
