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

#include <Storages/Transaction/TypeMapping.h>
#include <common/types.h>
#include <tipb/executor.pb.h>
#include <Core/Types.h>

namespace DB
{
class DAGContext;
using ColumnInfos = std::vector<TiDB::ColumnInfo>;

class TiDBFederationScan
{
public:
    TiDBFederationScan(
        const tipb::Executor * table_scan_,
        const String & executor_id_,
        const DAGContext & dag_context);
    const String & getTableScanExecutorID() const
    {
        return executor_id;
    }

    const tipb::Executor * getTableScanPB() const
    {
        return table_scan;
    }

private:
    const tipb::Executor * table_scan;
    String executor_id;
    Strings files;
};

} // namespace DB
