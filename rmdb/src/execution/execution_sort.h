/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<OrderCol> order_cols;
    size_t tuple_num;
    size_t limit;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;
    std::vector<std::unique_ptr<RmRecord>> tuples;
    bool is_end_{false};

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev,
                 std::vector<OrderCol> order_cols_,
                 int limit_) {
        prev_ = std::move(prev);
        limit = limit_;
        order_cols = std::move(order_cols_);
        tuple_num = 0;
        used_tuple.clear();
    }

    void beginTuple() override {
        // 清空已使用的元组
        used_tuple.clear();

        // 初始化前一个执行器的元组
        prev_->beginTuple();

        // 检查前一个执行器是否已结束，如果已结束，则当前执行器也结束
        if (prev_->is_end()) {
            is_end_ = true;
            return;
        } else {
            is_end_ = false;
        }

        // 遍历前一个执行器的所有记录，收集到tuples中
        for (; !prev_->is_end(); prev_->nextTuple()) {
            tuples.push_back(prev_->Next());
        }

        // 对收集到的记录进行排序
        std::sort(tuples.begin(), tuples.end(),
                  [this](const std::unique_ptr<RmRecord>& lhs,
                         const std::unique_ptr<RmRecord>& rhs) {
                      for (auto& order_col : order_cols) {
                          auto colMeta =
                              *get_col(prev_->cols(), order_col.tab_col);

                          int res = ix_compare(lhs->data + colMeta.offset,
                                               rhs->data + colMeta.offset,
                                               colMeta.type, colMeta.len);
                          if ((res == -1 && !order_col.is_desc_) ||
                              (res == 1 && order_col.is_desc_)) {
                              return true;
                          } else if (res == 0) {
                              continue;
                          } else {
                              return false;
                          }
                      }
                      return false;
                  });

        // 重置元组数量计数器
        tuple_num = 0;
    }

    void nextTuple() override {
        prev_->nextTuple();
        if (prev_->is_end()) {
            is_end_ = true;
        }
        tuple_num++;
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(tuples[tuple_num]);
    }

    bool is_end() const override {
        if (limit > 0 && tuple_num == limit) {
            return true;
        }
        return tuple_num == tuples.size();
    };

    const std::vector<ColMeta>& cols() const override { return prev_->cols(); };

    size_t tupleLen() const override { return prev_->tupleLen(); }

    Rid& rid() override { return _abstract_rid; }
};