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

#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Interpreters/Context.h>
#include <Storages/StorageFederation.h>

#include <algorithm>

namespace DB
{

class StorageFederationIterpreter
{
public:
    StorageFederationIterpreter(
        Context & context_,
        const TiDBTableScan & table_scan_,
        const FilterConditions & filter_conditions_,
        size_t max_streams_)
        : context(context_)
        , storage(std::make_unique<StorageFederation>(context_, table_scan_, table_scan_.getUris()))
        , max_streams(max_streams_)
        , log(Logger::get(context.getDAGContext()->log ? context.getDAGContext()->log->identifier() : ""))
        , filter_conditions(filter_conditions_)
    {}

    void execute(DAGPipeline & pipeline)
    {
        auto stage = QueryProcessingStage::Enum::FetchColumns;
        pipeline.streams = storage->read(Names(), SelectQueryInfo(), context, stage, 0, max_streams);
        analyzer = std::move(storage->analyzer);

        /// handle filter conditions for local and remote table scan.
        if (filter_conditions.hasValue())
        {
            ::DB::executePushedDownFilter(pipeline.streams.size(), filter_conditions, *analyzer, log, pipeline);
            //        recordProfileStreams(pipeline, filter_conditions.executor_id);
            auto & profile_streams = (*context.getDAGContext()).getProfileStreamsMap()[filter_conditions.executor_id];
            pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
        }
    }

    // Members will be transferred to DAGQueryBlockInterpreter after execute
    std::unique_ptr<DAGExpressionAnalyzer> analyzer;

private:
    Context & context;
    std::unique_ptr<StorageFederation> storage;
    size_t max_streams;

    LoggerPtr log;
    const FilterConditions & filter_conditions;
};
} // namespace DB
