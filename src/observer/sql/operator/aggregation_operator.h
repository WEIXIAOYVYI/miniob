/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by NiXianJun on 2023/9/10.
//

#pragma once

#include "sql/operator/operator.h"
#include "rc.h"

class Record;
class TupleCellSpec;

class AggregationOperator : public Operator
{
public:
  AggregationOperator()
  {}

  virtual ~AggregationOperator() = default;

  void add_aggr(const FieldMeta *field_meta, const AggregationType aggr);

  RC open() override;
  RC next() override;
  RC close() override;

  Tuple * current_tuple() override;

  int tuple_cell_num() const
  { 
    return tuple_.cell_num();
  }

  RC tuple_cell_spec_at(int index, const TupleCellSpec *&spec) const;

private:
  AggrTuple tuple_;
  int iter_index = 0;
  int count_star_start = -1;
  int count_star_num = 0;
  int bottom_num = 0;
};
