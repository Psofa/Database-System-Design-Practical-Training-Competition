/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator
                  

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is not on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }

        fed_conds_ = conds_;

    }


    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     */
    void beginTuple() override {
        scan_ = std::make_unique<RmScan>(fh_);
        // Debugging statement to ensure this method is working correctly

        while (!(scan_->is_end())) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                context_->lock_mgr_->lock_shared_on_record(context_->txn_, rid_, fh_->GetFd());
                break;
            }
            scan_->next();
        }
    }


    void nextTuple() override {
        scan_->next();
        while(!(scan_->is_end())) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                context_->lock_mgr_->lock_shared_on_record(context_->txn_, rid_, fh_->GetFd());
                break;
            }
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (scan_->is_end()) {
            return nullptr;
        } else {
            auto record = fh_->get_record(rid_, context_);

            return record;
        }         
    }

    void feed(const std::map<TabCol, Value> &feed_dict) {
        fed_conds_ = conds_;
        for (auto &cond : fed_conds_) {
            if (!cond.is_rhs_val && cond.rhs_col.tab_name != tab_name_) {
                cond.is_rhs_val = true;
                cond.rhs_val = feed_dict.at(cond.rhs_col);
            }
        }
    }

    Rid &rid() override { return rid_; }

    ColMeta get_col_offset(const TabCol &target) { 
        for (auto col : cols_) {
            if (target.col_name == col.name) {
                return col;
            }
        }
        return ColMeta();
    };

    bool is_end() const override { 
        return scan_->is_end(); 
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    std::string get_tab_name() { return tab_name_; }

    size_t get_len() { return len_; }
    
    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *rec) {
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        char *lhs = rec->data + lhs_col->offset;
        char *rhs;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            // value
            rhs_type = cond.rhs_val.type;
            rhs = cond.rhs_val.raw->data;
        } else {
            // column
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs_type = rhs_col->type;
            rhs = rec->data + rhs_col->offset;
        }
        
        // Check if type conversion is needed
        if (lhs_col->type != rhs_type) {
            float lhs_value_as_float, rhs_value_as_float;

            if (lhs_col->type == ColType::TYPE_INT && rhs_type == ColType::TYPE_FLOAT) {
                lhs_value_as_float = static_cast<float>(*reinterpret_cast<int*>(lhs));
                rhs_value_as_float = *reinterpret_cast<float*>(rhs);
            } else if (lhs_col->type == ColType::TYPE_FLOAT && rhs_type == ColType::TYPE_INT) {
                lhs_value_as_float = *reinterpret_cast<float*>(lhs);
                rhs_value_as_float = static_cast<float>(*reinterpret_cast<int*>(rhs));
            } else {
                // do nothing
            }

            int cmp = ix_compare(reinterpret_cast<char*>(&lhs_value_as_float), reinterpret_cast<char*>(&rhs_value_as_float), ColType::TYPE_FLOAT, sizeof(float));
            return eval_cmp_result(cond, cmp);
        } else {
            int cmp = ix_compare(lhs, rhs, rhs_type, lhs_col->len);
            return eval_cmp_result(cond, cmp);
        }
    }

    bool eval_cmp_result(const Condition &cond, int cmp) {
        if (cond.op == OP_EQ) {
            return cmp == 0;
        } else if (cond.op == OP_NE) {
            return cmp != 0;
        } else if (cond.op == OP_LT) {
            return cmp < 0;
        } else if (cond.op == OP_GT) {
            return cmp > 0;
        } else if (cond.op == OP_LE) {
            return cmp <= 0;
        } else if (cond.op == OP_GE) {
            return cmp >= 0;
        } else {
            throw InternalError("Unexpected op type");
        }
    }

    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const RmRecord *rec) {
        return std::all_of(conds.begin(), conds.end(),
                           [&](const Condition &cond) { return eval_cond(rec_cols, cond, rec); });    
    }

    bool is_single(const std::vector<Condition> &conds){
        return std::all_of(conds.begin(),conds.end(),[&](const Condition cond){
            return cond.op == OP_EQ;
        });
    }
};
