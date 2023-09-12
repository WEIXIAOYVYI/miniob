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
// Created by WangYunlai on 2022/6/7.
//

#pragma once

#include <iostream>
#include "storage/common/table.h"
#include "storage/common/field_meta.h"

class TupleCell
{
public: 
  TupleCell() = default;
  
  TupleCell(FieldMeta *meta, char *data)
    : TupleCell(meta->type(), data)
  {}
  TupleCell(AttrType attr_type, char *data)
    : attr_type_(attr_type), data_(data)
  {}

  void set_type(AttrType type) { this->attr_type_ = type; }
  void set_length(int length) { this->length_ = length; }
  void set_data(char *data) { this->data_ = data; }
  void set_data(const char *data) { this->set_data(const_cast<char *>(data)); }

  void to_string(std::ostream &os) const;

  int compare(const TupleCell &other) const;

  void zero(const AttrType &attr, int *acc_int, float *acc_float) {
    if (attr == INTS) {
      *acc_int = 0;
      attr_type_ = INTS;
      data_ = reinterpret_cast<char*>(acc_int);
    } else if (attr == FLOATS) {
      *acc_float = 0;
      attr_type_ = FLOATS;
      data_ = reinterpret_cast<char*>(acc_float);
    }
  }

  void add_one(int* acc_int) {
    (*acc_int)++;
    data_ = reinterpret_cast<char*>(acc_int);
  }

  void avg(const TupleCell &other, int count, float* acc_float) {
    if (other.attr_type() == INTS) {
      *acc_float= ((*acc_float) * count + *(int*)other.data()) / (count + 1);
    } else if (other.attr_type() == FLOATS) {
      *acc_float= ((*acc_float) * count + *(float*)other.data()) / (count + 1);
    }
    data_ = reinterpret_cast<char*>(acc_float);
  }

  const char *data() const
  {
    return data_;
  }

  int length() const { return length_; }

  AttrType attr_type() const
  {
    return attr_type_;
  }

private:
  AttrType attr_type_ = UNDEFINED;
  int length_ = -1;
  char *data_ = nullptr; // real data. no need to move to field_meta.offset
};