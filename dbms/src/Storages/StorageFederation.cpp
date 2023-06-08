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

#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Storages/StorageFederation.h>
#include <Storages/Transaction/TMTContext.h>

#include <iostream>
#include <memory>
#include <utility>

#include "Common/TiFlashException.h"
#include "Core/Names.h"
#include "Core/NamesAndTypes.h"
#include "DataStreams/ExpressionBlockInputStream.h"
#include "DataStreams/IBlockInputStream.h"
#include "Flash/Coprocessor/DAGExpressionAnalyzer.h"
#include "Flash/Coprocessor/DAGPipeline.h"
#include "Flash/Coprocessor/TiDBTableScan.h"
#include "Interpreters/Context.h"
#include "Interpreters/ExpressionActions.h"
#include "Storages/Federation/FederationInputStream.h"
#include "Storages/IStorage.h"
#include "common/defines.h"
#include "common/logger_useful.h"

namespace DB
{

StorageFederation::StorageFederation(
    Context & context_,
    const TiDBTableScan & table_scan_,
    const Strings & uris_)
    : IStorage()
    , table_scan(table_scan_)
    , context(context_)
    , uris(uris_)
    , log(Logger::get(context_.getDAGContext()->log ? context_.getDAGContext()->log->identifier() : ""))
{
    for (const auto & uri : uris)
        std::cout << "test1 : " + uri << std::endl;
}

BlockInputStreams StorageFederation::read(const Names &, const SelectQueryInfo &, [[maybe_unused]] const Context & context, [[maybe_unused]] QueryProcessingStage::Enum & processed_stage, [[maybe_unused]] size_t max_block_size, [[maybe_unused]] unsigned int num_streams)
{
    DAGPipeline pipeline;
    for (const auto & uri : uris)
        pipeline.streams.push_back(std::make_shared<FS::FederationInputStream>(log, uri, "parquet"));

    auto source_header = pipeline.firstStream()->getHeader();
    NamesAndTypesList input_column;
    for (const auto & column : source_header)
        input_column.emplace_back(column.name, column.type);

    Names column_name;
    for (const auto & name : table_scan.getNames())
    {
        auto name_and_type = source_header.getByName(name);
        column_name.emplace_back(name_and_type.name);
    }
    //    for (const auto & col : table_scan.getColumns())
    //    {
    //        std::cout << "col " << col.id << std::endl;
    //        auto name_and_type = source_header.safeGetByPosition(col.id);
    //        column_name.emplace_back(name_and_type.name);
    //    }

    ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column);
    project->add(ExpressionAction::project(column_name));

    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project, log->identifier()); });

    auto header = pipeline.firstStream()->getHeader();
    NamesAndTypes names_and_types;
    for (const auto & t : header.getNamesAndTypesList())
        names_and_types.emplace_back(t.name, t.type);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(names_and_types), context);
    return pipeline.streams;
}
} // namespace DB
