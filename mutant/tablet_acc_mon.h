#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>

#include "util/event_logger.h"

namespace rocksdb {

class _AccCnt;

class TabletAccMon {
  EventLogger* _logger = NULL;

  std::atomic<bool> _updatedSinceLastOutput;

  // SSTable access monitoring is for the SSTable migration decisions.
  // MemTable access monitoring is just to see/understand what's going on
  // internally.
  std::map<void*, _AccCnt*> _memtAccCnt;
  std::mutex _memtAccCntLock;
  std::map<uint64_t, _AccCnt*> _sstAccCnt;
  std::mutex _sstAccCntLock;

  std::thread _reporter;
  std::mutex _reporter_sleep_mutex;
  std::condition_variable _reporter_sleep_cv;
  bool _reporter_wakeupnow = false;

  static TabletAccMon& _GetInst();

  TabletAccMon();

  void _Init(EventLogger* logger);
  void _MemtRead(void* m);
  void _SstRead(uint64_t s);

  void _ReporterRun();
  void _ReporterSleep();
  void _ReporterWakeup();

public:
  static void Init(EventLogger* logger);
  static void MemtRead(void* m);
  static void SstRead(uint64_t s);
};

}
