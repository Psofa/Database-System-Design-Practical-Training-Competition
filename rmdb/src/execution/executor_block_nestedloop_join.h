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

class BlockNestedLoopJoinExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;    // 左表执行器
    std::unique_ptr<AbstractExecutor> right_;   // 右表执行器
    size_t len_;                                // join后每条记录的总长度
    std::vector<ColMeta> cols_;                 // join后记录的字段元数据

    std::vector<Condition> fed_conds_;          // join条件
    std::vector<std::pair<ColMeta, ColMeta>> join_cols_; // 连接的列对
    bool is_end_;                              // 是否已结束

    static constexpr int JOIN_POOL_SIZE = BUFFER_POOL_SIZE / 2; // join缓冲池大小
    static constexpr int TMP_FD = -2; // 临时文件描述符

    size_t left_len_; // 左表记录长度
    size_t right_len_; // 右表记录长度
    BufferPoolManager* bpm_; // 缓冲池管理器

    std::vector<Page*> right_buffer_pages_; // 右表缓冲页面
    int right_buffer_page_cnt_{0}; // 右表缓冲页面数量
    std::vector<Page*> left_buffer_pages_; // 左表缓冲页面
    int left_buffer_page_cnt_{0}; // 左表缓冲页面数量

    int left_buffer_page_iter_; // 当前左表缓冲页面的迭代器
    int left_buffer_page_inner_iter_; // 当前左表缓冲页面的内部迭代器
    int right_buffer_page_iter_; // 当前右表缓冲页面的迭代器
    int right_buffer_page_inner_iter_; // 当前右表缓冲页面的内部迭代器

    int left_num_per_page_; // 每页左表记录数量
    int right_num_per_page_; // 每页右表记录数量

    std::unordered_map<PageId, int> left_num_now_; // 记录左表数组中不同页面的记录数量
    std::unordered_map<PageId, int> right_num_now_; // 记录右表数组中不同页面的记录数量
    
    bool left_over{false}; // 左表是否已经处理完
    bool right_over{false}; // 右表是否已经处理完

    RmRecord join_record; // 用于存储连接结果的记录

    // 填充页面数据的辅助函数，用于填充左表和右表的缓冲页面
    bool fill_page(Page* page, std::unique_ptr<AbstractExecutor>& executor, size_t record_len, int& record_cnt, std::unordered_map<PageId, int>& num_now) {
        record_cnt = 0;
        while (!executor->is_end() && static_cast<size_t>(record_cnt) < (PAGE_SIZE / record_len)) {
            memcpy(page->get_data() + record_cnt * record_len, executor->Next()->data, record_len);
            record_cnt++;
            executor->nextTuple();
        }
        num_now[page->get_page_id()] = record_cnt;
        return executor->is_end();
    }

    // 初始化页面数组数据的辅助函数，用于填充左表和右表的缓冲页面数组
    void init_pages(std::vector<Page*>& pages, int& page_cnt, std::unique_ptr<AbstractExecutor>& executor, size_t record_len, std::unordered_map<PageId, int>& num_now) {
        executor->beginTuple();
        while (!executor->is_end() && pages.size() < JOIN_POOL_SIZE / 2) {
            PageId page_id{.fd = TMP_FD, .page_no = INVALID_PAGE_ID};
            auto page = bpm_->new_tmp_page(&page_id);
            pages.emplace_back(page);
            fill_page(page, executor, record_len, page_cnt, num_now);
        }
        page_cnt = pages.size();
    }

    // 重新填充页面数组数据的辅助函数
    void refill_pages(std::vector<Page*>& pages, int& page_cnt, std::unique_ptr<AbstractExecutor>& executor, size_t record_len, std::unordered_map<PageId, int>& num_now) {
        int new_page_cnt = 0;
        while (!executor->is_end() && new_page_cnt < page_cnt) {
            auto page = pages.at(new_page_cnt);
            fill_page(page, executor, record_len, new_page_cnt, num_now);
            new_page_cnt++;
        }
        page_cnt = new_page_cnt;
    }

    // 处理页面的辅助函数，用于查找左表和右表符合条件记录的连接结果
    bool process_pages(std::vector<Page*>& left_pages, int left_page_cnt, std::vector<Page*>& right_pages, int right_page_cnt) {
        while (left_buffer_page_iter_ < left_page_cnt) {
            auto left_page = left_pages[left_buffer_page_iter_];
            int left_num_now_inner_ = left_num_now_.find(left_page->get_page_id())->second;
            while (left_buffer_page_inner_iter_ < left_num_now_inner_) {
                memcpy(join_record.data, left_page->get_data() + left_buffer_page_inner_iter_ * left_len_, left_len_);
                while (right_buffer_page_iter_ < right_page_cnt) {
                    auto right_page = right_pages[right_buffer_page_iter_];
                    int right_num_now_inner_ = right_num_now_.find(right_page->get_page_id())->second;
                    while (right_buffer_page_inner_iter_ < right_num_now_inner_) {
                        memcpy(join_record.data + left_len_, right_page->get_data() + right_buffer_page_inner_iter_ * right_len_, right_len_);
                        right_buffer_page_inner_iter_++;
                        if (CheckConditions(join_record.data)) {
                            return true; // 如果条件满足，则返回true
                        }
                    }
                    right_buffer_page_iter_++;
                    right_buffer_page_inner_iter_ = 0;
                }
                right_buffer_page_iter_ = 0;
                left_buffer_page_inner_iter_++;
            }
            left_buffer_page_iter_++;
            left_buffer_page_inner_iter_ = 0;
        }
        return false;
    }

public:
    BlockNestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                                std::vector<Condition> conds, BufferPoolManager* bpm) {
        bpm_ = bpm;
        left_ = std::move(left);
        right_ = std::move(right);

        left_len_ = left_->tupleLen();
        left_num_per_page_ = PAGE_SIZE / left_len_;

        right_len_ = right_->tupleLen();
        right_num_per_page_ = PAGE_SIZE / right_len_;

        len_ = left_len_ + right_len_;
        join_record = RmRecord(len_);
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto& col : right_cols) {
            col.offset += left_len_;
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        is_end_ = false;
        fed_conds_ = std::move(conds);
        for (const auto& cond : fed_conds_) {
            assert(!cond.is_rhs_val);
            auto left_join_col = *get_col(cols_, cond.lhs_col);
            auto right_join_col = *get_col(cols_, cond.rhs_col);
            if (left_join_col.type != right_join_col.type) {
                throw IncompatibleTypeError(coltype2str(left_join_col.type), coltype2str(right_join_col.type));
            }
            join_cols_.emplace_back(left_join_col, right_join_col);
        }
    }

    void beginTuple() override {
        init_pages(right_buffer_pages_, right_buffer_page_cnt_, right_, right_len_, right_num_now_);
        init_pages(left_buffer_pages_, left_buffer_page_cnt_, left_, left_len_, left_num_now_);
        left_buffer_page_iter_ = 0;
        left_buffer_page_inner_iter_ = 0;
        right_buffer_page_iter_ = 0;
        right_buffer_page_inner_iter_ = 0;
        nextTuple();
    }

    void nextTuple() override {
        while (!left_over) {
            while (!right_over) {
                if (process_pages(left_buffer_pages_, left_buffer_page_cnt_, right_buffer_pages_, right_buffer_page_cnt_)) {
                    return; // 如果找到了符合条件的记录，则返回，直接退出到调用该函数的地方，即外面的for循环
                }
                if (right_->is_end()) {
                    right_over = true;
                    left_buffer_page_iter_ = 0;
                    continue; // 右表已经处理完，继续处理左表
                }
                refill_pages(right_buffer_pages_, right_buffer_page_cnt_, right_, right_len_, right_num_now_);
                left_buffer_page_iter_ = 0;
                left_buffer_page_inner_iter_ = 0;
            }
            if (left_->is_end()) {
                left_over = true;
                break; // 左表已经处理完，退出
            }
            refill_pages(left_buffer_pages_, left_buffer_page_cnt_, left_, left_len_, left_num_now_);
            right_->beginTuple(); // 重新初始化右表
            refill_pages(right_buffer_pages_, right_buffer_page_cnt_, right_, right_len_, right_num_now_);
            right_over = false;
        }
        is_end_ = true; // 标记结束
        for (auto left_page : left_buffer_pages_) {
            bpm_->unpin_tmp_page(left_page->get_page_id()); // 释放左表页面
        }
        for (auto right_page : right_buffer_pages_) {
            bpm_->unpin_tmp_page(right_page->get_page_id()); // 释放右表页面
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end_) {
            return nullptr; // 如果已经结束，返回nullptr
        }
        return std::make_unique<RmRecord>(join_record); // 返回当前的连接记录
    }

    Rid& rid() override { return _abstract_rid; }

    size_t tupleLen() const override {
        return len_; // 返回记录长度
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_; // 返回字段元数据
    }

    std::string getType() override {
        return "Block NestedLoop Join Executor"; // 返回执行器类型
    }

    bool is_end() const override {
        return is_end_; // 返回是否结束
    }

    ColMeta get_col_offset(const TabCol& target) override {
        return AbstractExecutor::get_col_offset(target); // 返回字段的偏移量
    }

    bool CheckConditions(const char* data) {
        bool result = true;
        auto join_size = fed_conds_.size();
        for (decltype(join_size) i = 0; i < join_size; i++) {
            result &= CheckCondition(data, i); // 检查每个条件
        }
        return result;
    }

    bool CheckCondition(const char* data, const size_t i) {
        const auto& left_col = join_cols_.at(i).first;
        const auto& right_col = join_cols_.at(i).second;
        const char* l_value = data + left_col.offset;
        const char* r_value = data + right_col.offset;
        int cmp = ix_compare(l_value, r_value, left_col.type, left_col.len);
        return eval_cmp_result(fed_conds_.at(i), cmp); // 判断条件是否成立
    }

    bool eval_cmp_result(const Condition& cond, int cmp) {
        switch (cond.op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp < 0;
            case OP_GT: return cmp > 0;
            case OP_LE: return cmp <= 0;
            case OP_GE: return cmp >= 0;
            default: throw InternalError("Unexpected op type"); // 未知的操作类型
        }
    }
};
