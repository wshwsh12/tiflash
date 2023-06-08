#include <IO/WriteBufferFromString.h>
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <common/types.h>
#include <fcntl.h>
#include <hdfs/hdfs.h>

#include "Poco/URI.h"

namespace DB::FS
{
class HDFSFileHandler
{
    static constexpr auto NAME = "HDFSFile";
    static constexpr auto schema_name = "hdfs";

public:
    explicit HDFSFileHandler(
        const String uri_)
        : uri(Poco::URI(uri_))
    {
        auto schema = uri.getScheme();
        if (schema != schema_name)
            return;
        hdfs_node = uri.getHost();
        hdfs_port = uri.getPort();
        file_path = uri.getPath();
        accessHDFSSystem();
    };

    HDFSFileHandler(
        String hdfs_node_,
        tPort hdfs_port_,
        String file_path_)
        : hdfs_node(hdfs_node_)
        , hdfs_port(hdfs_port_)
        , file_path(file_path_)
    {
        accessHDFSSystem();
    }

    std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile()
    {
        if (!file)
            return nullptr;
        std::string file_data;
        {
            WriteBufferFromString file_buffer(file_data);
            char buffer[BUFFER_SIZE];
            while (true)
            {
                auto n = hdfsRead(fs, file, &buffer, BUFFER_SIZE);
                if (n == -1)
                    return nullptr;
                if (n == 0)
                    break;
                file_buffer.write(reinterpret_cast<const char *>(&buffer), n);
            }
        }
        return std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(file_data)));
    }

    ~HDFSFileHandler()
    {
        if (file)
            hdfsCloseFile(fs, file);
        if (fs)
            hdfsDisconnect(fs);
    }

private:
    void accessHDFSSystem()
    {
        fs = hdfsConnect(hdfs_node.c_str(), hdfs_port);
        if (!fs)
            return;
        file = hdfsOpenFile(fs, file_path.c_str(), O_RDONLY, 0, 0, 0);
        if (!file)
            return;
    }
    static constexpr int BUFFER_SIZE = 1024;
    Poco::URI uri;
    String hdfs_node;
    tPort hdfs_port;
    String file_path;

    hdfsFS fs;
    hdfsFile file;
};

} // namespace DB::FS
