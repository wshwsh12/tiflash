// Copyright 2026 PingCAP, Inc.
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

#include <Storages/Tantivy/TiCIReadTaskPool.h>

#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
String rustBytesToString(const rust::Vec<uint8_t> & bytes)
{
    return String(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}
}

TEST(TiCIReadTaskPoolTest, FlattenBooleanQueryKeepsCurrentLevelContiguous)
{
    tipb::FTSQueryInfo info;
    auto * root = info.mutable_boolean_query();

    auto * group = root->add_nodes();
    group->set_occur(tipb::FTSBooleanOccur::FTSBooleanOccurMust);
    group->set_modifier(tipb::FTSBooleanModifier::FTSBooleanModifierNone);

    auto * baz = root->add_nodes();
    baz->set_occur(tipb::FTSBooleanOccur::FTSBooleanOccurShould);
    baz->set_modifier(tipb::FTSBooleanModifier::FTSBooleanModifierNone);
    baz->mutable_term()->set_term_type(tipb::FTSBooleanTermType::FTSBooleanTermWord);
    baz->mutable_term()->set_text("baz");

    auto * sub = group->mutable_sub_expression();
    auto * foo = sub->add_nodes();
    foo->set_occur(tipb::FTSBooleanOccur::FTSBooleanOccurMust);
    foo->set_modifier(tipb::FTSBooleanModifier::FTSBooleanModifierNone);
    foo->mutable_term()->set_term_type(tipb::FTSBooleanTermType::FTSBooleanTermWord);
    foo->mutable_term()->set_text("foo");

    auto * bar = sub->add_nodes();
    bar->set_occur(tipb::FTSBooleanOccur::FTSBooleanOccurMust);
    bar->set_modifier(tipb::FTSBooleanModifier::FTSBooleanModifierNone);
    bar->mutable_term()->set_term_type(tipb::FTSBooleanTermType::FTSBooleanTermWord);
    bar->mutable_term()->set_text("bar");

    rust::Vec<::BooleanQueryNode> nodes;
    DB::TS::appendTiCIBooleanNodesToFFI(info.boolean_query(), nodes);

    ASSERT_EQ(nodes.size(), 4);
    EXPECT_EQ(nodes[0].kind, 2);
    EXPECT_EQ(nodes[1].kind, 1);
    EXPECT_EQ(nodes[2].kind, 1);
    EXPECT_EQ(nodes[3].kind, 1);
    EXPECT_EQ(nodes[0].child_start, 2);
    EXPECT_EQ(nodes[0].child_len, 2);
    EXPECT_EQ(rustBytesToString(nodes[1].text), "baz");
    EXPECT_EQ(rustBytesToString(nodes[2].text), "foo");
    EXPECT_EQ(rustBytesToString(nodes[3].text), "bar");
}

TEST(TiCIReadTaskPoolTest, FlattenBooleanQueryCarriesPhraseDistance)
{
    tipb::FTSQueryInfo info;
    auto * root = info.mutable_boolean_query();

    auto * phrase = root->add_nodes();
    phrase->set_occur(tipb::FTSBooleanOccur::FTSBooleanOccurShould);
    phrase->set_modifier(tipb::FTSBooleanModifier::FTSBooleanModifierNone);
    phrase->mutable_term()->set_term_type(tipb::FTSBooleanTermType::FTSBooleanTermPhrase);
    phrase->mutable_term()->set_text("hello world");
    phrase->mutable_term()->set_phrase_distance(3);

    rust::Vec<::BooleanQueryNode> nodes;
    DB::TS::appendTiCIBooleanNodesToFFI(info.boolean_query(), nodes);

    ASSERT_EQ(nodes.size(), 1);
    EXPECT_EQ(nodes[0].kind, 1);
    EXPECT_EQ(rustBytesToString(nodes[0].text), "hello world");
    EXPECT_EQ(nodes[0].phrase_distance, 3);
}
} // namespace DB::tests
