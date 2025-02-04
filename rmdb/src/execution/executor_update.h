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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle* fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager* sm_manager_;

   public:
    UpdateExecutor(SmManager* sm_manager,
                   const std::string& tab_name,
                   std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds,
                   std::vector<Rid> rids,
                   Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        // 锁定表以进行独占访问
        context_->lock_mgr_->lock_exclusive_on_table(context_->txn_,
                                                     fh_->GetFd());

        // 检查类型
        for (auto& set_clause : set_clauses_) {
            auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
            if (lhs_col->type == TYPE_FLOAT &&
                set_clause.rhs.type == TYPE_INT) {
                set_clause.rhs.type = TYPE_FLOAT;
                set_clause.rhs.float_val = set_clause.rhs.int_val;
            } else if (lhs_col->type != set_clause.rhs.type) {
                throw IncompatibleTypeError(coltype2str(lhs_col->type),
                                            coltype2str(set_clause.rhs.type));
            }

            if (!set_clause.flag) {
                set_clause.rhs.init_raw(lhs_col->len);
            }
        }

        for (const auto& rid : rids_) {
            // 获取记录
            auto rec = fh_->get_record(rid, context_);

            // 记录更新前的记录
            RmRecord original_record = *rec;

            // 构造新数据
            for (size_t i = 0; i < set_clauses_.size(); i++) {
                auto col = tab_.get_col(set_clauses_[i].lhs.col_name);
                memcpy(rec->data + col->offset, set_clauses_[i].rhs.raw->data,
                       col->len);
            }
            RmRecord updated_record = *rec;

            // 更新记录文件中的记录
            fh_->update_record(rid, updated_record.data, context_);

        }

        return nullptr;
    }

    Rid& rid() override { return _abstract_rid; }
};