// Copyright 2025 PingCAP, Inc.
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

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/TimezoneInfo.h>
#include <Storages/Tantivy/TantivyInputStream.h>
#include <Storages/Tantivy/TiCIReadTaskPool.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
tipb::ColumnInfo makeQueryColumnInfo(Int64 column_id)
{
    tipb::ColumnInfo column;
    column.set_column_id(column_id);
    return column;
}

tipb::FTSBooleanNode makeTermNode(tipb::FTSBooleanOccur occur, tipb::FTSBooleanTermType term_type, const String & text)
{
    tipb::FTSBooleanNode node;
    node.set_occur(occur);
    node.set_modifier(tipb::FTSBooleanModifierNone);
    auto * term = node.mutable_term();
    term->set_term_type(term_type);
    term->set_text(text);
    return node;
}

tipb::Expr makeMatchWordExpr(const String & text, Int64 column_id)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::ScalarFunc);
    expr.set_sig(tipb::ScalarFuncSig::FTSMatchWord);

    auto * query_expr = expr.add_children();
    query_expr->set_tp(tipb::ExprType::String);
    query_expr->set_val(text);

    auto * column_expr = expr.add_children();
    column_expr->set_tp(tipb::ExprType::String);
    column_expr->set_val(fmt::format("column_{}", column_id));
    return expr;
}

std::vector<String> collectMatchTexts(const ::Expr & expr)
{
    std::vector<String> texts;
    if (expr.tp == tipb::ExprType::ScalarFunc)
    {
        if ((expr.sig == tipb::ScalarFuncSig::FTSMatchWord || expr.sig == tipb::ScalarFuncSig::FTSMatchPrefix
             || expr.sig == tipb::ScalarFuncSig::FTSMatchPhrase)
            && !expr.children.empty())
        {
            texts.emplace_back(expr.children[0].val.begin(), expr.children[0].val.end());
        }
        for (const auto & child : expr.children)
        {
            auto child_texts = collectMatchTexts(child);
            texts.insert(texts.end(), child_texts.begin(), child_texts.end());
        }
    }
    return texts;
}

bool containsSig(const ::Expr & expr, tipb::ScalarFuncSig sig)
{
    if (expr.sig == sig)
        return true;
    for (const auto & child : expr.children)
    {
        if (containsSig(child, sig))
            return true;
    }
    return false;
}
} // namespace

TEST(TiCIPhase1Test, BooleanQueryIgnoresShouldTermsWhenMustTermsExist)
{
    tipb::FTSQueryInfo query_info;
    *query_info.add_columns() = makeQueryColumnInfo(1);
    *query_info.add_columns() = makeQueryColumnInfo(2);
    *query_info.mutable_boolean_query()->add_nodes() = makeTermNode(
        tipb::FTSBooleanOccurMust,
        tipb::FTSBooleanTermWord,
        "must");
    *query_info.mutable_boolean_query()->add_nodes() = makeTermNode(
        tipb::FTSBooleanOccurShould,
        tipb::FTSBooleanTermPrefix,
        "optional");
    *query_info.mutable_boolean_query()->add_nodes() = makeTermNode(
        tipb::FTSBooleanOccurMustNot,
        tipb::FTSBooleanTermWord,
        "ban");

    TimezoneInfo timezone_info;
    timezone_info.init();
    auto [expr, cids] = TS::TiCIReadTaskPool::buildTiCIExprForTest(query_info, timezone_info);

    ASSERT_EQ(cids.size(), 2);
    EXPECT_EQ(cids[0], 1);
    EXPECT_EQ(cids[1], 2);
    EXPECT_EQ(expr.sig, tipb::ScalarFuncSig::LogicalAnd);
    EXPECT_TRUE(containsSig(expr, tipb::ScalarFuncSig::UnaryNotInt));

    auto texts = collectMatchTexts(expr);
    ASSERT_EQ(texts.size(), 2);
    EXPECT_EQ(texts[0], "must");
    EXPECT_EQ(texts[1], "ban");
}

TEST(TiCIPhase1Test, BooleanQueryUsesLogicalOrForShouldOnlyQueries)
{
    tipb::FTSQueryInfo query_info;
    *query_info.add_columns() = makeQueryColumnInfo(1);
    *query_info.mutable_boolean_query()->add_nodes() = makeTermNode(
        tipb::FTSBooleanOccurShould,
        tipb::FTSBooleanTermWord,
        "foo");
    *query_info.mutable_boolean_query()->add_nodes() = makeTermNode(
        tipb::FTSBooleanOccurShould,
        tipb::FTSBooleanTermPhrase,
        "bar baz");

    TimezoneInfo timezone_info;
    timezone_info.init();
    auto [expr, _] = TS::TiCIReadTaskPool::buildTiCIExprForTest(query_info, timezone_info);

    EXPECT_EQ(expr.sig, tipb::ScalarFuncSig::LogicalOr);
    auto texts = collectMatchTexts(expr);
    ASSERT_EQ(texts.size(), 2);
    EXPECT_EQ(texts[0], "foo");
    EXPECT_EQ(texts[1], "bar baz");
}

TEST(TiCIPhase1Test, BooleanQueryPreservesPrefixPhraseAndNestedGroup)
{
    tipb::FTSQueryInfo query_info;
    *query_info.add_columns() = makeQueryColumnInfo(1);

    auto * group = query_info.mutable_boolean_query()->add_nodes();
    group->set_occur(tipb::FTSBooleanOccurShould);
    group->set_modifier(tipb::FTSBooleanModifierNone);
    *group->mutable_sub_expression()->add_nodes() = makeTermNode(
        tipb::FTSBooleanOccurShould,
        tipb::FTSBooleanTermPrefix,
        "pre");
    *group->mutable_sub_expression()->add_nodes() = makeTermNode(
        tipb::FTSBooleanOccurShould,
        tipb::FTSBooleanTermPhrase,
        "foo bar");

    TimezoneInfo timezone_info;
    timezone_info.init();
    auto [expr, _] = TS::TiCIReadTaskPool::buildTiCIExprForTest(query_info, timezone_info);

    EXPECT_EQ(expr.sig, tipb::ScalarFuncSig::LogicalOr);
    EXPECT_TRUE(containsSig(expr, tipb::ScalarFuncSig::FTSMatchPrefix));
    EXPECT_TRUE(containsSig(expr, tipb::ScalarFuncSig::FTSMatchPhrase));

    auto texts = collectMatchTexts(expr);
    ASSERT_EQ(texts.size(), 2);
    EXPECT_EQ(texts[0], "pre");
    EXPECT_EQ(texts[1], "foo bar");
}

TEST(TiCIPhase1Test, BooleanQueryRejectsUnsupportedModifier)
{
    tipb::FTSQueryInfo query_info;
    *query_info.add_columns() = makeQueryColumnInfo(1);
    auto * node = query_info.mutable_boolean_query()->add_nodes();
    node->set_occur(tipb::FTSBooleanOccurShould);
    node->set_modifier(tipb::FTSBooleanModifierBoost);
    auto * term = node->mutable_term();
    term->set_term_type(tipb::FTSBooleanTermWord);
    term->set_text("foo");

    TimezoneInfo timezone_info;
    timezone_info.init();
    EXPECT_THROW(TS::TiCIReadTaskPool::buildTiCIExprForTest(query_info, timezone_info), DB::Exception);
}

TEST(TiCIPhase1Test, BooleanQueryRejectsPhraseDistance)
{
    tipb::FTSQueryInfo query_info;
    *query_info.add_columns() = makeQueryColumnInfo(1);
    auto * node = query_info.mutable_boolean_query()->add_nodes();
    node->set_occur(tipb::FTSBooleanOccurShould);
    node->set_modifier(tipb::FTSBooleanModifierNone);
    auto * term = node->mutable_term();
    term->set_term_type(tipb::FTSBooleanTermPhrase);
    term->set_text("foo bar");
    term->set_phrase_distance(3);

    TimezoneInfo timezone_info;
    timezone_info.init();
    EXPECT_THROW(TS::TiCIReadTaskPool::buildTiCIExprForTest(query_info, timezone_info), DB::Exception);
}

TEST(TiCIPhase1Test, BooleanQueryAndResidualMatchExprAreConjoined)
{
    tipb::FTSQueryInfo query_info;
    *query_info.add_columns() = makeQueryColumnInfo(1);
    *query_info.mutable_boolean_query()->add_nodes() = makeTermNode(
        tipb::FTSBooleanOccurShould,
        tipb::FTSBooleanTermWord,
        "foo");
    *query_info.add_match_expr() = makeMatchWordExpr("bar", 1);

    TimezoneInfo timezone_info;
    timezone_info.init();
    auto [expr, _] = TS::TiCIReadTaskPool::buildTiCIExprForTest(query_info, timezone_info);

    EXPECT_EQ(expr.sig, tipb::ScalarFuncSig::LogicalAnd);
    auto texts = collectMatchTexts(expr);
    ASSERT_EQ(texts.size(), 2);
    EXPECT_EQ(texts[0], "foo");
    EXPECT_EQ(texts[1], "bar");
}

TEST(TiCIPhase1Test, BooleanQueryMatchesNothingForMustNotOnlyQueriesWithoutSentinel)
{
    tipb::FTSQueryInfo query_info;
    *query_info.add_columns() = makeQueryColumnInfo(1);
    *query_info.mutable_boolean_query()->add_nodes() = makeTermNode(
        tipb::FTSBooleanOccurMustNot,
        tipb::FTSBooleanTermWord,
        "ban");

    TimezoneInfo timezone_info;
    timezone_info.init();
    auto [expr, _] = TS::TiCIReadTaskPool::buildTiCIExprForTest(query_info, timezone_info);

    EXPECT_EQ(expr.sig, tipb::ScalarFuncSig::LogicalAnd);
    EXPECT_TRUE(containsSig(expr, tipb::ScalarFuncSig::UnaryNotInt));

    auto texts = collectMatchTexts(expr);
    ASSERT_EQ(texts.size(), 2);
    EXPECT_EQ(texts[0], "ban");
    EXPECT_EQ(texts[1], "ban");
}

TEST(TiCIPhase1Test, ScoreSortRejectsVirtualScoreColumnId)
{
    EXPECT_THROW(
        TS::TantivyInputStream::validatePhase1SortColumnsOrThrow({TS::TantivyInputStream::virtual_score_column_id}),
        DB::Exception);
    EXPECT_NO_THROW(TS::TantivyInputStream::validatePhase1SortColumnsOrThrow({1, 2, 3}));
}

TEST(TiCIPhase1Test, ScoreOnlyProjectionUsesVersionPlaceholderReturnField)
{
    NamesAndTypes return_columns{
        {TS::TantivyInputStream::virtual_score_column_name, std::make_shared<DataTypeFloat64>()},
    };

    auto return_fields = TS::TantivyInputStream::buildReturnFieldsForTest(return_columns);
    ASSERT_EQ(return_fields.size(), 1);
    EXPECT_EQ(return_fields[0], TS::TantivyInputStream::version_column_name);
}

TEST(TiCIPhase1Test, ScoreProjectionReadsTopLevelDocumentScore)
{
    NamesAndTypes return_columns{
        {"column_1", std::make_shared<DataTypeString>()},
        {TS::TantivyInputStream::virtual_score_column_name, std::make_shared<DataTypeFloat64>()},
    };

    ::IdDocument document;
    document.fieldValues.push_back({});
    auto & title_field = document.fieldValues[0];
    title_field.field_name = "column_1";
    title_field.string_value.push_back('h');
    title_field.string_value.push_back('i');
    document.version = 7;
    document.score = 1.25;

    std::vector<::IdDocument> documents{document};
    auto block = TS::TantivyInputStream::fillResultBlockForTest(return_columns, documents);

    Field title_field_value;
    block.getByName("column_1").column->get(0, title_field_value);
    EXPECT_EQ(title_field_value.safeGet<String>(), "hi");

    Field score_field_value;
    block.getByName(TS::TantivyInputStream::virtual_score_column_name).column->get(0, score_field_value);
    EXPECT_DOUBLE_EQ(score_field_value.safeGet<Float64>(), 1.25);
}
} // namespace DB::tests
