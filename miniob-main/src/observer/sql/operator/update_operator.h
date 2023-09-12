#pragma once

#include "sql/operator/operator.h"
#include "sql/parser/parse.h"
#include "rc.h"

class UpdateStmt;
class Trx;

class UpdateOperator : public Operator
{
public:
  UpdateOperator(UpdateStmt *update_stmt,Trx *trx)
    : update_stmt_(update_stmt),trx_(trx)
  {}

  virtual ~UpdateOperator() = default;
  
  Tuple * current_tuple() override {
    return nullptr;
  }
  
  RC open() override;
  RC next() override;
  RC close() override;

private:
  UpdateStmt *update_stmt_ = nullptr;
  Trx *trx_ = nullptr;
};