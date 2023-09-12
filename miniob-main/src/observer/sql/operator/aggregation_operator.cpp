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
// Created by NiXianJun on 2023/09/10.
//

#include "common/log/log.h"
#include "sql/operator/aggregation_operator.h"
#include "storage/record/record.h"
#include "storage/common/table.h"

RC AggregationOperator::open()
{
    if (children_.size() != 1) {
        LOG_WARN("aggregation operator must has one child");
        return RC::INTERNAL;
    }

    Operator *child = children_[0];
    RC rc = child->open();
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to open child operator: %s", strrc(rc));
        return rc;
    }

    while ((rc = child->next()) == RC::SUCCESS) {
        Tuple * tuple = child->current_tuple();
        if (nullptr == tuple) {
            rc = RC::INTERNAL;
            LOG_WARN("failed to get current record. rc=%s", strrc(rc));
            break;
        }
        tuple_.update_cell(tuple, count_star_start, count_star_num);
    }
    return RC::SUCCESS;
}

RC AggregationOperator::next()
{   
    iter_index += 1;
    if (iter_index <= 1) {
        return RC::SUCCESS;
    }
    return RC::INTERNAL;
}

RC AggregationOperator::close()
{
    children_[0]->close();
    return RC::SUCCESS;
}

Tuple *AggregationOperator::current_tuple()
{   
    return &tuple_;
}

void AggregationOperator::add_aggr(const FieldMeta *field_meta, const AggregationType aggr)
{   
  bottom_num += 1;
  if (count_star_num != 0 && aggr == COUNTSTARAGGR) {
    count_star_num += 1;
    return;
  }

  char *prefix, *profix;
  profix = ")";
  TupleCellSpec *spec;
  AttrType attr = field_meta->type();

  switch (aggr) {
    case MAXAGGR :
        prefix = "MAX(";
        spec = new TupleCellSpec(new MaxExpr(attr));
        break;
    case MINAGGR:
        prefix = "MIN(";
        spec = new TupleCellSpec(new MinExpr(attr));
        break;
    case COUNTAGGR: case COUNTSTARAGGR:
        prefix = "COUNT(";
        spec = new TupleCellSpec(new CountExpr());
        break;
    case AVGAGGR:
        prefix = "AVG(";
        spec = new TupleCellSpec(new AvgExpr(attr));
        break;
    default:
        break;
  }

  char* name;
  if (aggr != COUNTSTARAGGR) {
    name = (char *) malloc(strlen(prefix) + strlen(field_meta->name()) + strlen(profix));
  } else {
    name = (char *) malloc(strlen(prefix) + 1+ strlen(profix));
  }
  strcpy(name, prefix);
  if (aggr != COUNTSTARAGGR) {
    strcat(name, field_meta->name());
  } else {
    strcat(name, "*");
    count_star_start = bottom_num - 1;
    count_star_num += 1;
  }
  strcat(name, profix);
  spec->set_alias(name);
  tuple_.add_cell_spec(spec);
}

RC AggregationOperator::tuple_cell_spec_at(int index, const TupleCellSpec *&spec) const
{   
    return tuple_.cell_spec_at(index, spec);
}
