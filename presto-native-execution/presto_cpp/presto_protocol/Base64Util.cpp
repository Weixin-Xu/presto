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
#include "presto_cpp/presto_protocol/Base64Util.h"
#include "bolt/common/encode/Base64.h"
//#include "bolt/common/memory/ByteStream.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
//#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

using namespace bytedance::bolt;
namespace facebook::presto::protocol {
namespace {

static const char* kLongArray = "LONG_ARRAY";
static const char* kIntArray = "INT_ARRAY";
static const char* kShortArray = "SHORT_ARRAY";
static const char* kByteArray = "BYTE_ARRAY";
static const char* kVariableWidth = "VARIABLE_WIDTH";
static const char* kRle = "RLE";
static const char* kArray = "ARRAY";
static const char* kMap = "MAP";
static const char* kInt128Array = "INT128_ARRAY";
static const __uint128_t kInt128Mask = ~(static_cast<__uint128_t>(1) << 127);

struct ByteStream {
  explicit ByteStream(const char* data, int32_t offset = 0)
      : data_(data), offset_(offset) {}

  template <typename T>
  T read() {
    // Directly reading int128 values is not yet supported in ByteStream.
    static_assert(sizeof(T) <= sizeof(uint64_t));
    T value = *reinterpret_cast<const T*>(data_ + offset_);
    offset_ += sizeof(T);
    return value;
  }

  std::string readString(int32_t size) {
    std::string value(data_ + offset_, size);
    offset_ += size;
    return value;
  }

  void readBytes(int32_t size, char* buffer) {
    memcpy(buffer, data_ + offset_, size);
    offset_ += size;
  }

 private:
  const char* data_;
  int32_t offset_;
};

// ByteStream::read specialization for int128_t
template <>
int128_t ByteStream::read<int128_t>() {
  // Fetching int128_t value by reading two 64-bit blocks rather than one
  // 128-bit block to avoid general protection exception.
  auto low = read<int64_t>();
  auto high = read<int64_t>();
  return HugeInt::build(high, low);
}

BufferPtr
readNulls(int32_t count, ByteStream& stream, memory::MemoryPool* pool) {
  bool mayHaveNulls = stream.read<bool>();
  if (!mayHaveNulls) {
    return nullptr;
  }

  BufferPtr nulls = AlignedBuffer::allocate<bool>(count, pool);

  auto numBytes = bytedance::bolt::bits::nbytes(count);
  stream.readBytes(numBytes, nulls->asMutable<char>());

  bits::reverseBits(nulls->asMutable<uint8_t>(), numBytes);
  bits::negate(nulls->asMutable<char>(), count);
  return nulls;
}

template <typename T, typename U>
VectorPtr readScalarBlock(
    const TypePtr& type,
    ByteStream& stream,
    memory::MemoryPool* pool) {
  auto positionCount = stream.read<int32_t>();

  BufferPtr nulls = readNulls(positionCount, stream, pool);
  const uint64_t* rawNulls = nulls == nullptr ? nullptr : nulls->as<uint64_t>();

  BufferPtr buffer =
      AlignedBuffer::allocate<T>(positionCount, pool);
  auto rawBuffer = buffer->asMutable<T>();
  for (auto i = 0; i < positionCount; i++) {
    if (!rawNulls || !bits::isBitNull(rawNulls, i)) {
      rawBuffer[i] = stream.read<T>();
    }
  }
  if (type->isLongDecimal()) {
    for (auto i = 0; i < positionCount; i++) {
      // Convert signed magnitude form to 2's complement.
      if (rawBuffer[i] < 0) {
        rawBuffer[i] &= kInt128Mask;
        rawBuffer[i] *= -1;
      }
    }
  }

  switch (type->kind()) {
    case TypeKind::BIGINT:
    case TypeKind::INTEGER:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT:
    case TypeKind::DOUBLE:
    case TypeKind::REAL:
    case TypeKind::VARCHAR:
    case TypeKind::HUGEINT:
      return std::make_shared<FlatVector<U>>(
          pool,
          type,
          nulls,
          positionCount,
          buffer,
          std::vector<BufferPtr>{});
    case TypeKind::TIMESTAMP: {
      BufferPtr timestamps =
          AlignedBuffer::allocate<Timestamp>(positionCount, pool);
      auto* rawTimestamps = timestamps->asMutable<Timestamp>();
      for (auto i = 0; i < positionCount; i++) {
        rawTimestamps[i] = Timestamp(
            rawBuffer[i] / 1'000'000, (rawBuffer[i] % 1'000'000) * 1'000);
      }
      return std::make_shared<FlatVector<Timestamp>>(
          pool,
          type,
          nulls,
          positionCount,
          timestamps,
          std::vector<BufferPtr>{});
    }
    case TypeKind::BOOLEAN: {
      BufferPtr bits =
          AlignedBuffer::allocate<bool>(positionCount, pool);
      auto* rawBits = bits->asMutable<uint64_t>();
      for (auto i = 0; i < positionCount; i++) {
        bits::setBit(rawBits, i, rawBuffer[i] != 0);
      }
      return std::make_shared<FlatVector<bool>>(
          pool,
          type,
          nulls,
          positionCount,
          bits,
          std::vector<BufferPtr>{});
    }
    default:
      BOLT_FAIL("Unexpected Block type: {}" + type->toString());
  }
}

VectorPtr readVariableWidthBlock(
    const TypePtr& type,
    ByteStream& stream,
    memory::MemoryPool* pool) {
  auto positionCount = stream.read<int32_t>();

  BufferPtr offsets =
      AlignedBuffer::allocate<int32_t>(positionCount + 1, pool);
  auto rawOffsets = offsets->asMutable<int32_t>();
  rawOffsets[0] = 0;
  for (auto i = 0; i < positionCount; i++) {
    rawOffsets[i + 1] = stream.read<int32_t>();
  }

  auto nulls = readNulls(positionCount, stream, pool);

  auto totalSize = stream.read<int32_t>();

  BufferPtr stringBuffer =
      AlignedBuffer::allocate<char>(totalSize, pool);
  auto rawString = stringBuffer->asMutable<char>();
  stream.readBytes(totalSize, rawString);

  BufferPtr buffer =
      AlignedBuffer::allocate<StringView>(positionCount, pool);
  auto rawBuffer = buffer->asMutable<StringView>();
  for (auto i = 0; i < positionCount; i++) {
    auto size = rawOffsets[i + 1] - rawOffsets[i];
    rawBuffer[i] = StringView(rawString + rawOffsets[i], size);
  }

  return std::make_shared<FlatVector<StringView>>(
      pool,
      type,
      nulls,
      positionCount,
      buffer,
      std::vector<BufferPtr>{stringBuffer});
}

template <TypeKind Kind>
VectorPtr readScalarBlock(
    const std::string& encoding,
    const TypePtr& type,
    ByteStream& stream,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<Kind>::NativeType;

  if (encoding == kLongArray) {
    return readScalarBlock<int64_t, T>(type, stream, pool);
  }

  if (encoding == kIntArray) {
    return readScalarBlock<int32_t, T>(type, stream, pool);
  }

  if (encoding == kShortArray) {
    return readScalarBlock<int16_t, T>(type, stream, pool);
  }

  if (encoding == kByteArray) {
    return readScalarBlock<int8_t, T>(type, stream, pool);
  }

  if (encoding == kVariableWidth) {
    return readVariableWidthBlock(type, stream, pool);
  }

  if (encoding == kInt128Array) {
    return readScalarBlock<int128_t, T>(type, stream, pool);
  }
  BOLT_UNREACHABLE();
}

VectorPtr readRleBlock(
    const TypePtr& type,
    ByteStream& stream,
    memory::MemoryPool* pool) {
  // read number of rows - must be just one
  auto positionCount = stream.read<int32_t>();

  // skip the encoding of the values
  auto encodingLength = stream.read<int32_t>();
  auto encoding = stream.readString(encodingLength);

  auto innerCount = stream.read<int32_t>();
  BOLT_CHECK_EQ(
      innerCount,
      1,
      "Unexpected RLE block. Expected single inner position. Got {}",
      innerCount);

  auto nulls = readNulls(1, stream, pool);
  if (!nulls || !bits::isBitNull(nulls->as<uint64_t>(), 0)) {
    throw std::runtime_error("Unexpected RLE block. Expected single null.");
  }

  return BaseVector::createNullConstant(type, positionCount, pool);
}

void unpackTimestampWithTimeZone(
    int64_t packed,
    int64_t& timestamp,
    int16_t& timezone) {
  timestamp = packed >> 12;
  timezone = packed & 0xfff;
}

// Common code to read number of rows, nulls and offsets for ARRAY and MAP.
auto readArrayOrMapFinalPart(
    ByteStream& stream,
    memory::MemoryPool* pool) {
  auto positionCount = stream.read<int32_t>();

  BufferPtr offsets =
      AlignedBuffer::allocate<int32_t>(positionCount + 1, pool);
  auto rawOffsets = offsets->asMutable<int32_t>();
  for (auto i = 0; i < positionCount + 1; i++) {
    rawOffsets[i] = stream.read<int32_t>();
  }

  BufferPtr nulls = readNulls(positionCount, stream, pool);

  BufferPtr sizes =
      AlignedBuffer::allocate<int32_t>(positionCount, pool);
  auto rawSizes = sizes->asMutable<int32_t>();
  for (auto i = 0; i < positionCount; i++) {
    rawSizes[i] = rawOffsets[i + 1] - rawOffsets[i];
  }

  struct result {
    BufferPtr nulls;
    int32_t positionCount;
    BufferPtr offsets;
    BufferPtr sizes;
  };
  return result{nulls, positionCount, offsets, sizes};
}

VectorPtr readBlockInt(
    const TypePtr& type,
    ByteStream& stream,
    memory::MemoryPool* pool) {
  // read the encoding
  auto encodingLength = stream.read<int32_t>();
  std::string encoding = stream.readString(encodingLength);

  if (encoding == kArray) {
    auto elements = readBlockInt(type->asArray().elementType(), stream, pool);

    auto [nulls, positionCount, offsets, sizes] =
        readArrayOrMapFinalPart(stream, pool);
    const auto arrayType = ARRAY(elements->type());
    return std::make_shared<ArrayVector>(
        pool, arrayType, nulls, positionCount, offsets, sizes, elements);
  }

  if (encoding == kMap) {
    auto keys = readBlockInt(type->asMap().keyType(), stream, pool);
    auto values = readBlockInt(type->asMap().valueType(), stream, pool);

    // We aren't using hashtable.
    auto hashtableSize = stream.read<int32_t>();
    for (auto i = 0; i < hashtableSize; i++) {
      stream.read<int32_t>();
    }

    auto [nulls, positionCount, offsets, sizes] =
        readArrayOrMapFinalPart(stream, pool);
    const auto mapType = MAP(keys->type(), values->type());
    return std::make_shared<MapVector>(
        pool, mapType, nulls, positionCount, offsets, sizes, keys, values);
  }

  if (encoding == kRle) {
    return readRleBlock(type, stream, pool);
  }

  if (type->kind() == TypeKind::HUGEINT) {
    return readScalarBlock<TypeKind::HUGEINT>(
        encoding, type, stream, pool);
  }

  if (type->kind() == TypeKind::ROW &&
      isTimestampWithTimeZoneType(type)) {
    auto positionCount = stream.read<int32_t>();

    auto timestamps =
        BaseVector::create(BIGINT(), positionCount, pool);
    auto rawTimestamps =
        timestamps->asFlatVector<int64_t>()->mutableRawValues();

    auto timezones =
        BaseVector::create(SMALLINT(), positionCount, pool);
    auto rawTimezones = timezones->asFlatVector<int16_t>()->mutableRawValues();

    BufferPtr nulls = readNulls(positionCount, stream, pool);
    const uint64_t* rawNulls =
        nulls == nullptr ? nullptr : nulls->as<uint64_t>();

    for (auto i = 0; i < positionCount; i++) {
      if (!rawNulls || !bits::isBitNull(rawNulls, i)) {
        int64_t unpacked = stream.read<int64_t>();
        unpackTimestampWithTimeZone(
            unpacked, rawTimestamps[i], rawTimezones[i]);
      }
    }

    return std::make_shared<RowVector>(
        pool,
        TIMESTAMP_WITH_TIME_ZONE(),
        nulls,
        positionCount,
        std::vector<VectorPtr>{timestamps, timezones});
  }

  if (type->kind() == TypeKind::ROW) {
    auto numFields = stream.read<int32_t>();
    std::vector<VectorPtr> children;
    children.reserve(numFields);
    for (auto i = 0; i < numFields; i++) {
      children.push_back(readBlockInt(type->asRow().childAt(i), stream, pool));
    }
    auto positionCount = stream.read<int32_t>();
    for (int position = 0; position < positionCount + 1; position++) {
      stream.read<int32_t>();
    }
    BufferPtr nulls = readNulls(positionCount, stream, pool);
    return std::make_shared<RowVector>(
        pool, type, nulls, positionCount, children);
  }

  return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      readScalarBlock, type->kind(), encoding, type, stream, pool);
}

} // namespace

VectorPtr readBlock(
    const TypePtr& type,
    const std::string& base64Encoded,
    memory::MemoryPool* pool) {
  const std::string data = encoding::Base64::decode(base64Encoded);

  ByteStream stream(data.data());
  return readBlockInt(type, stream, pool);
}


} // namespace facebook::presto::protocol
