#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <fmt/os.h>

#include <algorithm>
#include <memory>
#include <string>

#include "IO/WriteBufferFromString.h"
#include "IO/copyData.h"
#include "hdfs/hdfs.h"
namespace DB
{
std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(
    ReadBuffer & in)
{
    std::string file_data;
    {
        WriteBufferFromString file_buffer(file_data);
        copyData(in, file_buffer);
    }
    return std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(file_data)));
}

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(
    hdfsFS hdfs_fs,
    hdfsFile hdfs_file)
{
    std::string file_data;
    {
        WriteBufferFromString file_buffer(file_data);
        char buffer[10240000];
        while (true)
        {
            auto n = hdfsRead(hdfs_fs, hdfs_file, &buffer, 10240000);
            if (n == -1)
                return nullptr;
            if (n == 0)
                break;
            file_buffer.write(reinterpret_cast<const char *>(&buffer), n);
            std::cout << n << std::endl;
        }
    }
    std::cout << file_data.size() << std::endl;
    return std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(file_data)));
}
} // namespace DB
