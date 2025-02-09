#!/bin/bash

# パラメータのデフォルト値
DATASET_TYPES=("sift")

# sift と gist 用のパラメータ
LARGE_INITIAL_NUMS=(0 100 1000 5000 10000 50000 100000 200000)
LARGE_POSTING_LIMITS=(5 10 20 50 100 200 500 1000)
LARGE_CONNECTION_LIMITS=(5 10 20 50 100 200 500)
LARGE_INSERT_THREADS=(1 2 4 8 16 36 72)
LARGE_SEARCH_THREADS=(1 2 4 8 16 36 72)
LARGE_TOP_KS=(1 10 50 100 200 500)
LARGE_NODE_NUMS=(1 2 5 10 20 50 100 200 500)
# LARGE_PQ_SIZES=(10 20)

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

    for i in {1..3}; do
        # 実行コマンド
        CMD="./benchmarks/bench_serial --dataset_type=${dataset_type} --initial_num=${initial_num} --posting_limit=${posting_limit} --connection_limit=${connection_limit} --insert_threads=${insert_threads} --search_threads=${search_threads} --top_k=${top_k} --node_num=${node_num} --pq_size=${node_num} --output_file=${TIMESTAMP}"

        # コマンドを表示して実行
        echo "Running command: ${CMD}"
        ${CMD}
    done
}

# パラメータを変えながら実行
for dataset_type in "${DATASET_TYPES[@]}"; do
    for initial_num in "${LARGE_INITIAL_NUMS[@]}"; do
        for posting_limit in "${LARGE_POSTING_LIMITS[@]}"; do
            for connection_limit in "${LARGE_CONNECTION_LIMITS[@]}"; do
                for insert_threads in "${LARGE_INSERT_THREADS[@]}"; do
                    for search_threads in "${LARGE_SEARCH_THREADS[@]}"; do
                        for top_k in "${LARGE_TOP_KS[@]}"; do
                            for node_num in "${LARGE_NODE_NUMS[@]}"; do
                                run_benchmarks $dataset_type $initial_num $posting_limit $connection_limit $insert_threads $search_threads $top_k $node_num
                            done
                        done
                    done
                done
            done
        done
    done
done
