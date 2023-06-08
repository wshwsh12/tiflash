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

#include <Common/Logger.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

#include "Core/Types.h"
#include "Flash/Coprocessor/TiDBTableScan.h"
#include "IO/WriteHelpers.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <tipb/executor.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
class StorageFederation : public IStorage
{
public:
    StorageFederation(
        Context & context_,
        const TiDBTableScan & table_scan_,
        const Strings & uris);

    std::string getName() const override
    {
        return "StorageFederation";
    }

    std::string getTableName() const override
    {
        return "StorageFederation_file_num_" + toString(uris.size());
    }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    // Members will be transferred to DAGQueryBlockInterpreter after execute
    std::unique_ptr<DAGExpressionAnalyzer> analyzer;

private:
    const TiDBTableScan table_scan;
    [[maybe_unused]] Context & context;
    const Strings uris;
    LoggerPtr log;
};
} // namespace DB
