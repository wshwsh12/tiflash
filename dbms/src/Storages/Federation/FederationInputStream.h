// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <arrow/api.h>
#include <arrow/chunked_array.h>
#include <arrow/filesystem/hdfs.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/hdfs.h>
#include <arrow/io/interfaces.h>
#include <arrow/scalar.h>
#include <arrow/type_fwd.h>
#include <fcntl.h>
#include <fmt/os.h>
#include <hdfs/hdfs.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>
#include <parquet/types.h>

#include <filesystem>
#include <iostream>
#include <memory>

#include "Columns/ColumnString.h"
#include "Columns/ColumnVector.h"
#include "Common/Exception.h"
#include "Common/Logger.h"
#include "Common/assert_cast.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/DataTypesNumber.h"
#include "Storages/Federation/ArrowBufferedStreams.h"
#include "Storages/Federation/HDFSFileHandler.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/io/hdfs.h"
#include "arrow/util/uri.h"
#include "common/logger_useful.h"

namespace DB::FS
{
class FederationInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "FederationInputStream";

public:
    FederationInputStream(
        LoggerPtr log_,
        const String & uri_,
        const String & file_type_)
        : uri(uri_)
        , file_type(file_type_)
        , log(log_)
    {
        auto hdfs_file = std::make_shared<HDFSFileHandler>(uri);
        std::unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(
            parquet::arrow::OpenFile(hdfs_file->asArrowFile(), arrow::default_memory_pool(), &reader));
        PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
        header = arrowSchemaToCHHeader(*table->schema());
        //   LOG_DEBUG(log, file_name + file_type);
        //   std::shared_ptr<arrow::io::ReadableFile> infile;
        //   PARQUET_ASSIGN_OR_THROW(
        //       infile,
        //       arrow::io::ReadableFile::Open("27401376-43c6-439f-8002-1ef1bb44f0a0-0_0-16-12_20230313075258622.parquet",
        //                                     arrow::default_memory_pool()));

        //   std::cout << infile->GetSize().ValueOrDie() << std::endl;
        //   std::unique_ptr<parquet::arrow::FileReader> reader;
        //   PARQUET_THROW_NOT_OK(
        //       parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
        //   std::shared_ptr<arrow::Table> table;
        //   PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
        //   header = arrowSchemaToCHHeader(*table->schema());
    }

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        if (done)
        {
            return {};
        }
        done = true;
        return readFromFile();
    }

private:
    Block readFromFile()
    {
        auto schema = table->schema()->field_names();

        Block res;
        for (int i = 0; i < table->num_columns(); i++)
        {
            auto arrow_col = table->column(i);
            res.insert(readOneColumn(arrow_col, schema[i]));
        }
        return res;
    }

#define FOR_ARROW_NUMERIC_TYPES(M)          \
    M(arrow::Type::UINT8, DB::UInt8)        \
    M(arrow::Type::INT8, DB::Int8)          \
    M(arrow::Type::INT16, DB::Int16)        \
    M(arrow::Type::INT32, DB::Int32)        \
    M(arrow::Type::UINT64, DB::UInt64)      \
    M(arrow::Type::INT64, DB::Int64)        \
    M(arrow::Type::DURATION, DB::Int64)     \
    M(arrow::Type::HALF_FLOAT, DB::Float32) \
    M(arrow::Type::FLOAT, DB::Float32)      \
    M(arrow::Type::DOUBLE, DB::Float64)

    static ColumnWithTypeAndName readOneColumn(
        std::shared_ptr<arrow::ChunkedArray> & arrow_column,
        const std::string & column_name)
    {
        switch (arrow_column->type()->id())
        {
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            return readColumnWithStringData<arrow::BinaryArray>(arrow_column, column_name);
#define DISPATCH(ARROW_NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
    case ARROW_NUMERIC_TYPE:                           \
        return readColumnWithNumericData<CPP_NUMERIC_TYPE>(arrow_column, column_name);
            FOR_ARROW_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        default:
            throw Exception("???");
        }
        return {};
    }
    /// Inserts numeric data right into internal column data to reduce an overhead
    template <typename NumericType, typename VectorType = ColumnVector<NumericType>>
    static ColumnWithTypeAndName readColumnWithNumericData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
    {
        auto internal_type = std::make_shared<DataTypeNumber<NumericType>>();
        auto internal_column = internal_type->createColumn();
        auto & column_data = static_cast<VectorType &>(*internal_column).getData();
        column_data.reserve(arrow_column->length());

        for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
        {
            std::shared_ptr<arrow::Array> chunk = arrow_column->chunk(chunk_i);
            if (chunk->length() == 0)
                continue;

            /// buffers[0] is a null bitmap and buffers[1] are actual values
            std::shared_ptr<arrow::Buffer> buffer = chunk->data()->buffers[1];
            const auto * raw_data = reinterpret_cast<const NumericType *>(buffer->data());
            column_data.insert_assume_reserved(raw_data, raw_data + chunk->length());
        }
        return {std::move(internal_column), std::move(internal_type), column_name};
    }

    template <typename ArrowArray>
    static ColumnWithTypeAndName readColumnWithStringData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
    {
        auto internal_type = std::make_shared<DataTypeString>();
        auto internal_column = internal_type->createColumn();
        PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(*internal_column).getChars();
        PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*internal_column).getOffsets();

        size_t chars_t_size = 0;
        for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
        {
            auto & chunk = dynamic_cast<ArrowArray &>(*(arrow_column->chunk(chunk_i)));
            const size_t chunk_length = chunk.length();

            if (chunk_length > 0)
            {
                chars_t_size += chunk.value_offset(chunk_length - 1) + chunk.value_length(chunk_length - 1);
                chars_t_size += chunk_length; /// additional space for null bytes
            }
        }

        column_chars_t.reserve(chars_t_size);
        column_offsets.reserve(arrow_column->length());

        for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
        {
            auto & chunk = dynamic_cast<ArrowArray &>(*(arrow_column->chunk(chunk_i)));
            std::shared_ptr<arrow::Buffer> buffer = chunk.value_data();
            const size_t chunk_length = chunk.length();

            for (size_t offset_i = 0; offset_i != chunk_length; ++offset_i)
            {
                if (!chunk.IsNull(offset_i) && buffer)
                {
                    const auto * raw_data = buffer->data() + chunk.value_offset(offset_i);
                    column_chars_t.insert_assume_reserved(raw_data, raw_data + chunk.value_length(offset_i));
                }
                column_chars_t.emplace_back('\0');

                column_offsets.emplace_back(column_chars_t.size());
            }
        }
        return {std::move(internal_column), std::move(internal_type), column_name};
    }

    static void checkStatus(const arrow::Status & status)
    {
        if (!status.ok())
            throw Exception("Error with a {} column '{}': {}.");
    }

    static Block arrowSchemaToCHHeader(
        const arrow::Schema & schema)
    {
        ColumnsWithTypeAndName sample_columns;
        std::unordered_set<String> nested_table_names;

        for (const auto & field : schema.fields())
        {
            /// Create empty arrow column by it's type and convert it to ClickHouse column.
            arrow::MemoryPool * pool = arrow::default_memory_pool();
            std::unique_ptr<arrow::ArrayBuilder> array_builder;
            arrow::Status status = MakeBuilder(pool, field->type(), &array_builder);
            checkStatus(status);

            std::shared_ptr<arrow::Array> arrow_array;
            status = array_builder->Finish(&arrow_array);
            checkStatus(status);

            arrow::ArrayVector array_vector = {arrow_array};
            auto arrow_column = std::make_shared<arrow::ChunkedArray>(array_vector);
            ColumnWithTypeAndName sample_column = readOneColumn(
                arrow_column,
                field->name());
            sample_columns.push_back(sample_column);
        }
        return Block(std::move(sample_columns));
    }
    const String & uri;
    std::shared_ptr<arrow::Table> table;
    [[maybe_unused]] const String & file_type;

    Block header;
    bool done = false;
    LoggerPtr log;
};

} // namespace DB::FS
