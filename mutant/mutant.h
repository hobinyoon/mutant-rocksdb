#pragma once

#include <condition_variable>
#include <map>
#include <mutex>
#include <deque>
#include <set>
#include <thread>

#include <boost/date_time/posix_time/posix_time.hpp>

#include "rocksdb/options.h"


namespace rocksdb {

struct BlockBasedTable;
class ColumnFamilyData;
class DBImpl;
class EventLogger;
struct FileDescriptor;
struct FileMetaData;
struct JSONWriter;
class MemTable;
class TableReader;

class SstTemp;

// Mutant design
// -------------
// Flush job thread:
// - When a flush is done, updates Mutant with the creation (thus newly opened)
//   of output SSTables so that Mutant can monitor.
//
// Compaction job thread:
// - queries Mutant for the temperature level of the input SSTables.
// - When a compaction is done, updates Mutant with the creation (thus opened)
//   of output SSTables so that Mutant can monitor.
//
// Which thread ?: You can easily trace
// - When an existing SSTable is open, update Mutant with the open SSTables.
//
// Read thread: updates SSTable access count
//
// Temperature-update thread:
//   pulls SSTable accesses periodically and updates their temperature.
//
// Migration triggerer thread:
//   checks (hot) SSTable temperatures periodically and when then become cold,
//   triggers migration.


class Mutant {
  // Let's keep a copy here. It gets SIGSEGV everywhere. I suspect the undeterministic behavior is form the race from a lot of the DB
  // threads. If we kept a pointer here, we are not sure of the lifetime of the _options.
  DBOptions::MutantOptions _options;
  DBImpl* _db = nullptr;
  EventLogger* _logger = nullptr;
  ColumnFamilyData* _cfd = nullptr;
  //DiskMon* _disk_mon = nullptr;

  // This is updated very frequently by threads whenever a SSTable or a
  // MemTable is read, thus we don't used an expensive atomic operation here.
  //
  // It only affects the temp updater thread when to update, and it's ok that
  // it's delayed by a tiny little bit, due to the relaxed cache coherency.
  bool _updatedSinceLastOutput = false;

  // SSTable access monitoring is for the SSTable migration decisions.
  // MemTable access monitoring is just to see/understand what's going on
  // internally.
  //
  // We keep only active memt and sst lists here. (Read) access countings are
  // done with a counter in each memt and sst.
  //
  // Note: disable MemTable monitoring when measuring the monitoring overhead.
  std::set<MemTable*> _memtSet;
  std::mutex _memtSetLock_OpenedClosed;
  std::mutex _memtSetLock;

  // map<sst_id, SstTemp*>
  std::map<uint64_t, SstTemp*> _sstMap;
  // The first lock is held when reading/updating SstTemp
  // The second lock is held when modifying the map
  // See _SstClosed() why this 2-level locking is needed. A deadlock happens without this.
  std::mutex _sstMapLock_OpenedClosed;
  std::mutex _sstMapLock;

  std::thread* _temp_updater_thread = nullptr;
  std::mutex _temp_updater_sleep_mutex;
  std::condition_variable _temp_updater_sleep_cv;
  bool _temp_updater_wakeupnow = false;

  std::mutex _temp_updating_mutex;
  bool _temp_updated = false;
  std::condition_variable _temp_updated_cv;

  bool _temp_updater_stop_requested = false;

  // SSTable migration triggerer
  std::thread* _smt_thread = nullptr;;
  std::mutex _smt_sleep_mutex;
  std::condition_variable _smt_sleep_cv;
  bool _smt_wakeupnow = false;

  bool _smt_stop_requested = false;

  bool _initialized = false;

  //SlaAdmin* _sla_admin = nullptr;
  //double _target_lat = -1.0;
  //std::mutex _lat_hist_lock;
  //std::deque<double> _lat_hist;
  //std::mutex _slow_dev_r_iops_hist_lock;
  //std::deque<double> _slow_dev_r_iops_hist;

  std::mutex _last_sst_write_time_lock;
  boost::posix_time::ptime _last_sst_write_time;
  int _num_running_compactions = 1000;
  int _num_running_flushes = 1000;

  // Greedy knapsack based SSTable organization
  std::mutex _sstOrgLock;
  // SSTable IDs that need to go to the fast storage or slow storage.
  std::set<uint64_t> _ssts_must_be_in_fast;
  std::multimap<double, uint64_t> _ssts_must_be_in_fast_by_temp;
  std::set<uint64_t> _ssts_must_be_in_slow;
  std::multimap<double, uint64_t> _ssts_must_be_in_slow_by_temp;
  // The threshold in between the fast SSTables and slow SSTables after an organization. -1 when undefined.
  double _sst_ott = -1;

  static Mutant& _GetInst();

  void _Init(const DBOptions::MutantOptions* mo, DBImpl* db, EventLogger* el);
  void _MemtCreated(ColumnFamilyData* cfd, MemTable* m);
  void _MemtDeleted(MemTable* m);
  void _SstOpened(TableReader* tr, const FileDescriptor* fd, int level);
  void _SstClosed(BlockBasedTable* bbt);
  void _RunTempUpdaterAndWait();
  void _SetUpdated();
  double _SstTemperature(uint64_t sst_id, const boost::posix_time::ptime& cur_time);
  int _SstLevel(uint64_t sst_id);
  double _SstTemp(uint64_t sst_id);

  uint32_t _CalcOutputPathId(
      bool temperature_triggered_single_sstable_compaction,
      const std::vector<FileMetaData*>& file_metadata,
      const std::vector<FileMetaData*>& file_metadata1,
      int output_level);
  uint32_t _CalcOutputPathIdTrivialMove(const FileMetaData* fmd);

  FileMetaData* __GetSstFileMetaDataForMigration(const uint64_t sst_id, int& level_for_migration);
  FileMetaData*_PickSstToMigrate(int& level_for_migration);

  void __SstOrgGreedyKnapsack(bool log);

  void _TempUpdaterRun();
  void _TempUpdaterSleep();
  void _TempUpdaterWakeup();

  void _SstMigrationTriggererRun();
  void _SstMigrationTriggererSleep();
  void _SstMigrationTriggererWakeup();

  //void _SlaAdminInit(double target_lat, double p, double i, double d);
  //void _SlaAdminAdjust(double lat);
  //void _AdjSstOtt(double cur_value, const boost::posix_time::ptime& cur_time, JSONWriter* jwriter);
  //void _LogSstStatus(const boost::posix_time::ptime& cur_time, JSONWriter* jwriter);

  void _SetNumRunningCompactions(int n);
  void _SetNumRunningFlushes(int n);

  void _Shutdown();

  const DBOptions::MutantOptions* _Options();


protected:
  Mutant();


public:
  static void Init(const DBOptions::MutantOptions* mo, DBImpl* db, EventLogger* el);
  static void MemtCreated(ColumnFamilyData* cfd, MemTable* m);
  static void MemtDeleted(MemTable* m);
  static void SstOpened(TableReader* tr, const FileDescriptor* fd, int level);
  static void SstClosed(BlockBasedTable* bbt);
  static void SetUpdated();

  static uint32_t CalcOutputPathId(
      bool temperature_triggered_single_sstable_compaction,
      const std::vector<FileMetaData*>& file_metadata,
      const std::vector<FileMetaData*>& file_metadata1,
      int output_level);
  static uint32_t CalcOutputPathIdTrivialMove(const FileMetaData* fmd);

  static FileMetaData* PickSstToMigrate(int& level_for_migration);

  //static void SlaAdminInit(double target_lat, double p, double i, double d);

  // We play with the average read latency. It is a client-observed latency, but can be embedded in the DB as well.
  //   An advantage of the client-observed one is that the DB doesn't need to know the specifics of the storage such as cost.
  //     It can be configured from outside.
  //static void SlaAdminAdjust(double lat);

  static void SetNumRunningCompactions(int n);
  static void SetNumRunningFlushes(int n);

  static void Shutdown();

  static const DBOptions::MutantOptions* Options();
};

}
