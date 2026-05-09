/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/update_physical_operator.h"

#include "common/log/log.h"
#include "storage/field/field_meta.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const FieldMeta *field, Value value)
    : table_(table), field_(field), value_(std::move(value))
{}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  unique_ptr<PhysicalOperator> &child = children_[0];

  RC rc = child->open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (tuple == nullptr) {
      LOG_WARN("failed to get current tuple");
      child->close();
      return RC::INTERNAL;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record old_record;
    old_record.copy_data(row_tuple->record().data(), row_tuple->record().len());
    old_record.set_rid(row_tuple->record().rid());

    Record new_record;
    new_record.copy_data(old_record.data(), old_record.len());
    new_record.set_rid(old_record.rid());

    size_t copy_len = field_->len();
    if (is_string_type(field_->type()) && copy_len > static_cast<size_t>(value_.length())) {
      copy_len = value_.length() + 1;
    }
    memset(new_record.data() + field_->offset(), 0, field_->len());
    memcpy(new_record.data() + field_->offset(), value_.data(), copy_len);

    records_.emplace_back(std::move(old_record), std::move(new_record));
  }

  child->close();

  if (rc != RC::RECORD_EOF) {
    LOG_WARN("failed to get next record. rc=%s", strrc(rc));
    return rc;
  }

  for (auto &record_pair : records_) {
    rc = trx->update_record(table_, record_pair.first, record_pair.second);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to update record. rc=%s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  records_.clear();
  return RC::SUCCESS;
}
