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
#pragma once

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/VectorStream.h"

namespace facebook::presto::operators {

/// Struct for single broadcast file info.]
// This struct is a 1:1 strict API mapping to
// presto-spark-base/src/main/java/com/facebook/presto/spark/execution/BroadcastFileInfo.java
// Please refrain from making changes to this API class. If any changes have to
// be made to this struct, one should make sure to make corresponding changes in
// the above Java classes and its corresponding serde functionalities.
struct BroadcastFileInfo {
  std::string filePath_;
  // TODO: Add additional stats including checksum, num rows, size.

  static std::unique_ptr<BroadcastFileInfo> deserialize(
      const std::string& info);
};

/// Writes broadcast data to a file.
class BroadcastFileWriter {
 public:
  BroadcastFileWriter(
      std::string_view filename,
      const bolt::RowTypePtr& inputType,
      std::shared_ptr<bolt::filesystems::FileSystem> fileSystem,
      bolt::memory::MemoryPool* pool);

  virtual ~BroadcastFileWriter() = default;

  /// Write to file.
  void collect(const bolt::RowVectorPtr& input);

  /// Flush the data.
  void noMoreData();

  /// Returns file name if non zero rows written to file.
  /// Returns nullptr if there were no rows written.
  bolt::RowVectorPtr fileStats();

 private:
  /// Initializes write file.
  void initializeWriteFile();

  /// Serializes input rowVector using PrestoVectorSerde and
  /// writes serialized data to file.
  void write(const bolt::RowVectorPtr& rowVector);

  std::unique_ptr<bolt::WriteFile> writeFile_;
  std::shared_ptr<bolt::filesystems::FileSystem> fileSystem_;
  std::string filename_;
  int64_t numRows_;
  int64_t maxSerializedSize_;
  bolt::memory::MemoryPool* pool_;
  std::unique_ptr<bolt::VectorSerde> serde_;
  const bolt::RowTypePtr& inputType_;
};

/// Reads broadcast data back from files.
class BroadcastFileReader {
 public:
  BroadcastFileReader(
      std::unique_ptr<BroadcastFileInfo>& broadcastFileInfo,
      std::shared_ptr<bolt::filesystems::FileSystem> fileSystem,
      bolt::memory::MemoryPool* pool);

  ~BroadcastFileReader() = default;

  /// Return true if more data is available.
  bool hasNext();

  /// Read next block of data.
  bolt::BufferPtr next();

  /// Reader stats - number of bytes.
  folly::F14FastMap<std::string, int64_t> stats();

 private:
  std::unique_ptr<BroadcastFileInfo> broadcastFileInfo_;
  std::shared_ptr<bolt::filesystems::FileSystem> fileSystem_;
  bool hasData_;
  int64_t numBytes_;
  bolt::memory::MemoryPool* pool_;
};

/// Factory to create Writers & Reader for file based broadcast.
class BroadcastFactory {
 public:
  BroadcastFactory(const std::string& basePath);

  virtual ~BroadcastFactory() = default;

  std::unique_ptr<BroadcastFileWriter> createWriter(
      bolt::memory::MemoryPool* pool,
      const bolt::RowTypePtr& inputType);

  std::shared_ptr<BroadcastFileReader> createReader(
      const std::unique_ptr<BroadcastFileInfo> fileInfo,
      bolt::memory::MemoryPool* pool);

 private:
  const std::string basePath_;
  std::shared_ptr<bolt::filesystems::FileSystem> fileSystem_;
};
} // namespace facebook::presto::operators
