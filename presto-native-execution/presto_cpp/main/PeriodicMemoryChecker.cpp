/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "presto_cpp/main/PeriodicMemoryChecker.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/main/common/Utils.h"
#include "bolt/common/base/StatsReporter.h"
#include "bolt/common/caching/AsyncDataCache.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/time/Timer.h"

namespace facebook::presto {
PeriodicMemoryChecker::PeriodicMemoryChecker(Config config)
    : config_(std::move(config)) {
  if (config_.systemMemPushbackEnabled) {
    BOLT_CHECK_GT(config_.systemMemLimitBytes, 0);
  }
  if (config_.mallocMemHeapDumpEnabled) {
    BOLT_CHECK(
        !config_.heapDumpLogDir.empty(),
        "heapDumpLogDir cannot be empty when heap dump is enabled.");
    BOLT_CHECK(
        !config_.heapDumpFilePrefix.empty(),
        "heapDumpFilePrefix cannot be empty when heap dump is enabled.");
  }
}

void PeriodicMemoryChecker::start() {
  if (!config_.systemMemPushbackEnabled) {
    PRESTO_STARTUP_LOG(INFO) << "Server memory pushback is not enabled";
  } else {
    BOLT_CHECK_GT(
        config_.systemMemLimitBytes, 0, "Invalid system mem limit provided");
    BOLT_CHECK_GT(
        config_.systemMemShrinkBytes, 0, "Invalid system mem shrink provided");
    PRESTO_STARTUP_LOG(INFO)
        << "Creating server memory pushback checker, memory check interval "
        << config_.memoryCheckerIntervalMs << "ms, system memory limit: "
        << bytedance::bolt::succinctBytes(config_.systemMemLimitBytes)
        << ", memory shrink size: "
        << bytedance::bolt::succinctBytes(config_.systemMemShrinkBytes);
  }

  if (!config_.mallocMemHeapDumpEnabled) {
    PRESTO_STARTUP_LOG(INFO) << "Malloc memory heap dumper is not enabled";
  } else {
    PRESTO_STARTUP_LOG(INFO)
        << "Enabling Malloc memory heap dumper"
        << ", malloc'd memory dump threshold: "
        << bytedance::bolt::succinctBytes(config_.mallocBytesUsageDumpThreshold)
        << ", max dump files: " << config_.maxHeapDumpFiles
        << ", heap dump folder: " << config_.heapDumpLogDir
        << ", heap dump interval: " << config_.minHeapDumpIntervalSec
        << " seconds";
  }

  BOLT_CHECK_NULL(scheduler_, "start() called more than once");
  scheduler_ = std::make_shared<folly::FunctionScheduler>();
  scheduler_->setThreadName("MemoryCheckerThread");
  scheduler_->addFunction(
      [&]() {
        periodicCb();
        if (config_.mallocMemHeapDumpEnabled) {
          maybeDumpHeap();
        }
        if (config_.systemMemPushbackEnabled &&
            systemUsedMemoryBytes() > config_.systemMemLimitBytes) {
          pushbackMemory();
        }
      },
      std::chrono::milliseconds(config_.memoryCheckerIntervalMs),
      "periodic-sys-mem-check",
      std::chrono::seconds(0));
  scheduler_->start();
}

void PeriodicMemoryChecker::stop() {
  BOLT_CHECK_NOT_NULL(scheduler_);
  scheduler_->shutdown();
  scheduler_.reset();
}

std::string PeriodicMemoryChecker::createHeapDumpFilePath() const {
  const size_t now = bytedance::bolt::getCurrentTimeMs() / 1000;
  // Format as follow:
  // <heapDumpFilePrefix>.<pid>.<global_sequence>.i<sequence> =>
  // prefix.1234.235.i565
  return fmt::format(
      "{}/{}.{}.{}.i{}.heap",
      config_.heapDumpLogDir,
      config_.heapDumpFilePrefix,
      getpid(),
      now,
      now);
}

void PeriodicMemoryChecker::maybeDumpHeap() {
  const auto now = bytedance::bolt::getCurrentTimeMs() / 1000;
  const auto allocatedSize = mallocBytes();
  if (allocatedSize >= config_.mallocBytesUsageDumpThreshold &&
      now - lastHeapDumpAttemptTimestamp_ >= config_.minHeapDumpIntervalSec) {
    lastHeapDumpAttemptTimestamp_ = now;
    LOG(INFO) << fmt::format(
        "Memory usage allocated via malloc exceeded threshold of {}, current "
        "allocation: {}",
        bytedance::bolt::succinctBytes(config_.mallocBytesUsageDumpThreshold),
        bytedance::bolt::succinctBytes(allocatedSize));

    const auto minMemUsageDumped = dumpFilesByHeapMemUsageMinPq_.empty()
        ? 0
        : dumpFilesByHeapMemUsageMinPq_.top().mallocUsedBytes;
    if (dumpFilesByHeapMemUsageMinPq_.size() == config_.maxHeapDumpFiles &&
        allocatedSize <= minMemUsageDumped) {
      LOG(INFO) << fmt::format(
          "Heap profile not dumped as current usage {} is below the "
          "minimum usage dumped {} and we already have {} files in rotation",
          bytedance::bolt::succinctBytes(allocatedSize),
          bytedance::bolt::succinctBytes(minMemUsageDumped),
          config_.maxHeapDumpFiles);
      return;
    }

    const auto filePath = createHeapDumpFilePath();
    if (!heapDumpCb(filePath)) {
      LOG(ERROR) << "Error dumping Heap profile";
      return;
    }
    LOG(INFO) << fmt::format(
        "Heap profile with usage {} dumped to {}",
        bytedance::bolt::succinctBytes(allocatedSize),
        filePath);

    dumpFilesByHeapMemUsageMinPq_.push({allocatedSize, filePath});
    if (dumpFilesByHeapMemUsageMinPq_.size() <= config_.maxHeapDumpFiles) {
      return;
    }
    auto& evicted = dumpFilesByHeapMemUsageMinPq_.top();
    LOG(INFO) << fmt::format(
        "Removing Heap profile with lowest usage {} : {}",
        bytedance::bolt::succinctBytes(evicted.mallocUsedBytes),
        evicted.filePath);
    removeDumpFile(evicted.filePath.c_str());
    dumpFilesByHeapMemUsageMinPq_.pop();
  }
}

void PeriodicMemoryChecker::pushbackMemory() {
  RECORD_METRIC_VALUE(kCounterMemoryPushbackCount);
  const uint64_t currentMemBytes = systemUsedMemoryBytes();
  BOLT_CHECK(config_.systemMemPushbackEnabled);
  LOG(WARNING) << "System used memory " << bytedance::bolt::succinctBytes(currentMemBytes)
               << " exceeded limit: "
               << bytedance::bolt::succinctBytes(config_.systemMemLimitBytes);
  const uint64_t targetMemBytes =
      config_.systemMemLimitBytes - config_.systemMemShrinkBytes;
  BOLT_CHECK_GT(currentMemBytes, targetMemBytes);
  const uint64_t bytesToShrink = currentMemBytes - targetMemBytes;
  BOLT_CHECK_GT(bytesToShrink, 0);

  uint64_t latencyUs{0};
  uint64_t freedBytes{0};
  {
    bytedance::bolt::MicrosecondTimer timer(&latencyUs);
    auto* cache = bytedance::bolt::cache::AsyncDataCache::getInstance();
    auto systemConfig = SystemConfig::instance();
    freedBytes = cache != nullptr ? cache->shrink(bytesToShrink) : 0;
    if (freedBytes < bytesToShrink) {
      try {
        auto* memoryManager = bytedance::bolt::memory::memoryManager();
        freedBytes += bytedance::bolt::memory::AllocationTraits::pageBytes(
            memoryManager->allocator()->unmap(
                bytedance::bolt::memory::AllocationTraits::numPages(
                    bytesToShrink - freedBytes)));
        if (freedBytes < bytesToShrink &&
            systemConfig->systemMemPushBackAbortEnabled()) {
          memoryManager->shrinkPools(
              bytesToShrink - freedBytes,
              /*allowSpill=*/false,
              /*allowAbort=*/true);

          // Try to shrink from cache again as aborted query might hold cache
          // reference.
          if (cache != nullptr) {
            freedBytes += cache->shrink(bytesToShrink - freedBytes);
          }
          if (freedBytes < bytesToShrink) {
            freedBytes += bytedance::bolt::memory::AllocationTraits::pageBytes(
                memoryManager->allocator()->unmap(
                    bytedance::bolt::memory::AllocationTraits::numPages(
                        bytesToShrink - freedBytes)));
          }
        }
      } catch (const bytedance::bolt::BoltException& ex) {
        LOG(ERROR) << ex.what();
      }
    }
  }
  RECORD_HISTOGRAM_METRIC_VALUE(
      kCounterMemoryPushbackLatencyMs, latencyUs / 1000);
  const auto actualFreedBytes = std::max<int64_t>(
      0, static_cast<int64_t>(currentMemBytes) - systemUsedMemoryBytes());
  RECORD_HISTOGRAM_METRIC_VALUE(
      kCounterMemoryPushbackExpectedReductionBytes, freedBytes);
  RECORD_HISTOGRAM_METRIC_VALUE(
      kCounterMemoryPushbackReductionBytes, actualFreedBytes);
  LOG(INFO) << "Memory pushback shrunk " << bytedance::bolt::succinctBytes(freedBytes)
            << " Effective bytes shrunk: "
            << bytedance::bolt::succinctBytes(actualFreedBytes);
}

#ifndef PRESTO_MEMORY_CHECKER_TYPE
// Initialize singleton for the checker to be nullptr if
// PRESTO_MEMORY_CHECKER_TYPE is not defined.
folly::Singleton<facebook::presto::PeriodicMemoryChecker> checker([]() {
  return nullptr;
});
#endif
} // namespace facebook::presto
