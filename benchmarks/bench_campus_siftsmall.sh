#!/bin/bash

# パラメータのデフォルト値
DATASET_TYPES=("siftsmall")

# siftsmall 用のパラメータ
SIFTSMALL_INITIAL_NUMS=(0 100 1000 2000)
SIFTSMALL_POSTING_LIMITS=(5 10 20 50 100 200 500)
SIFTSMALL_CONNECTION_LIMITS=(5 10 20 50 100)
SIFTSMALL_INSERT_THREADS=(1 2 4 8 16 32 72)
SIFTSMALL_SEARCH_THREADS=(1 2 4 8 16 32 72)
SIFTSMALL_TOP_KS=(1 10 50 100)
SIFTSMALL_NODE_NUMS=(1 2 5 10 20 50)   
# SIFTSMALL_PQ_SIZES=(10 20)
SIFTSMALL_DELETE_ARCHIVED=(True False)

# スクリプト実行時の日時を取得
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# 実行関数
run_benchmarks() {
    local dataset_type=$1
    local initial_num=$2
    local posting_limit=$3
    local connection_limit=$4
    local insert_threads=$5
    local search_threads=$6
    local top_k=$7
    local node_num=$8
    local delete_archived=$9

    for i in {1..3}; do
        # 実行コマンド
        CMD="./benchmarks/bench_campus --dataset_type=${dataset_type} --initial_num=${initial_num} --posting_limit=${posting_limit} --connection_limit=${connection_limit} --insert_threads=${insert_threads} --search_threads=${search_threads} --top_k=${top_k} --node_num=${node_num} --pq_size=${node_num} --output_file=${TIMESTAMP} --delete_archived=${delete_archived}"

        # コマンドを表示して実行
        echo "Running command: ${CMD}"
        ${CMD}
    done
}

# パラメータを変えながら実行
for dataset_type in "${DATASET_TYPES[@]}"; do
    for initial_num in "${SIFTSMALL_INITIAL_NUMS[@]}"; do
        for posting_limit in "${SIFTSMALL_POSTING_LIMITS[@]}"; do
            for connection_limit in "${SIFTSMALL_CONNECTION_LIMITS[@]}"; do
                for insert_threads in "${SIFTSMALL_INSERT_THREADS[@]}"; do
                    for search_threads in "${SIFTSMALL_SEARCH_THREADS[@]}"; do
                        for top_k in "${SIFTSMALL_TOP_KS[@]}"; do
                            for node_num in "${SIFTSMALL_NODE_NUMS[@]}"; do
                                for delete_archived in "${SIFTSMALL_DELETE_ARCHIVED[@]}"; do
                                    run_benchmarks $dataset_type $initial_num $posting_limit $connection_limit $insert_threads $search_threads $top_k $node_num $delete_archived
                                done
                            done
                        done
                    done
                done
            done
        done
    done
done
