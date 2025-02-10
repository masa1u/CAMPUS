#include "../src/campus/campus.h"
#include <iostream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include <algorithm>
#include <thread>
#include <chrono>
#include <fstream>
#include <string>
#include <cassert>
#include <unordered_set>
#include <filesystem>
#include <gflags/gflags.h>

DEFINE_string(dataset_type, "siftsmall", "Dataset type (siftsmall, sift, gist)");
DEFINE_int32(initial_num, 1000, "Initial number of vectors");
// parameters for Campus index itself
DEFINE_int32(posting_limit, 100, "Posting limit");
DEFINE_int32(connection_limit, 10, "Connection limit");

DEFINE_int32(insert_threads, 1, "Number of threads for insertion");

// parameters for search operation
DEFINE_int32(search_threads, 1, "Number of threads for search");
DEFINE_bool(delete_archived, true, "Delete archived nodes before search");
DEFINE_int32(top_k, 100, "Number of top k elements to search");
DEFINE_int32(node_num, 10, "Number of nodes to search");
DEFINE_int32(pq_size, 10, "Priority queue size");

DEFINE_string(output_file, "", "Output csv file path");


// データをL2正規化する関数
void normalizeDataset(std::vector<std::vector<float>> &dataset) {
    for (auto &vec : dataset) {
        float norm = 0.0f;
        for (const auto &value : vec) {
            norm += value * value; // L2ノルムの計算
        }
        norm = std::sqrt(norm);

        // ノルムが0の場合を回避
        if (norm > 0) {
            for (auto &value : vec) {
                value /= norm; // 各要素をノルムで割る
            }
        }
    }
}

// fvecsファイルを読み込む関数
std::vector<std::vector<float>> readFvecs(const std::string &file_path) {
    std::vector<std::vector<float>> vectors;
    std::ifstream input(file_path, std::ios::binary);
    if (!input) {
        std::cerr << "Error opening file: " << file_path << std::endl;
        return vectors;
    }

    while (input) {
        int dim;
        input.read(reinterpret_cast<char*>(&dim), sizeof(int));
        if (!input) break;

        std::vector<float> vec(dim);
        input.read(reinterpret_cast<char*>(vec.data()), dim * sizeof(float));
        vectors.push_back(vec);
    }

    return vectors;
}

// ivecsファイルを読み込む関数
std::vector<std::vector<int>> readIvecs(const std::string &file_path) {
    std::vector<std::vector<int>> vectors;
    std::ifstream input(file_path, std::ios::binary);
    if (!input) {
        std::cerr << "Error opening file: " << file_path << std::endl;
        return vectors;
    }

    while (input) {
        int dim;
        input.read(reinterpret_cast<char*>(&dim), sizeof(int));
        if (!input) break;

        std::vector<int> vec(dim);
        input.read(reinterpret_cast<char*>(vec.data()), dim * sizeof(int));
        vectors.push_back(vec);
    }

    return vectors;
}

// ベクトルを挿入する関数
void insertVectors(int thread_id, int &ready, const bool &start_flag, Campus *campus, const std::vector<std::vector<float>> &vectors, int start, int end) {
    __atomic_store_n(&ready, 1, __ATOMIC_SEQ_CST);
    while (!start_flag) {
        std::this_thread::yield();
    }

    for (int i = start; i < end; ++i) {
        CampusInsertExecutor insert_executor(campus, static_cast<const void*>(vectors[i].data()), i);
        insert_executor.insert();
    }
}

// 類似ベクトル検索を行う関数
void searchVectors(int thread_id, int &ready, const bool &start_flag, Campus *campus, const std::vector<std::vector<float>> &queries, int start, int end, int top_k, std::vector<std::vector<int>> &results) {
    __atomic_store_n(&ready, 1, __ATOMIC_SEQ_CST);
    while (!start_flag) {
        std::this_thread::yield();
    }

    for (int i = start; i < end; ++i) {
        CampusQueryExecutor query_executor(campus, static_cast<const void*>(queries[i].data()), FLAGS_top_k, FLAGS_node_num, FLAGS_pq_size);
        std::vector<int> result = query_executor.query();
        results[i] = result;
    }
}

// リコールを計算する関数
float calculateRecall(const std::vector<std::vector<int>> &results, const std::vector<std::vector<int>> &groundtruth) {
    int correct = 0;
    int total = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        std::unordered_set<int> groundtruth_set(groundtruth[i].begin(), groundtruth[i].begin() + results[i].size());
        for (int id : results[i]) {
            if (groundtruth_set.find(id) != groundtruth_set.end()) {
                ++correct;
            }
        }
        total += results[i].size();
    }
    return static_cast<float>(correct) / total;
}

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::string base_file, query_file, groundtruth_file, output_file;
    std::string current_path = std::filesystem::current_path().string();

    if (FLAGS_output_file.empty()) {
        std::cerr << "Output file is not specified." << std::endl;
        return 1;
    }
    // output_fileにパラメータをwrite
    output_file = current_path + "/../benchmarks/results/" + FLAGS_output_file + ".csv";
    std::ofstream ofs(output_file, std::ios::app);
    if (ofs.tellp() == 0) {
        ofs << "index_type,dataset_type,initial_num,posting_limit,connection_limit,insert_threads,search_threads,delete_archived,top_k,node_num,pq_size,initial_node_num,insert_throughput,insert_latency,all_vectors,unique_vectors,violated_vectors,search_throughput,search_latency,recall\n";
    }
    ofs << "campus," << FLAGS_dataset_type << "," << FLAGS_initial_num << "," << FLAGS_posting_limit << "," << FLAGS_connection_limit << "," 
        << FLAGS_insert_threads << "," << FLAGS_search_threads << "," << FLAGS_delete_archived << "," << FLAGS_top_k << "," << FLAGS_node_num << "," << FLAGS_pq_size << ",";
    ofs.flush();
    ofs.close();

    if (FLAGS_dataset_type == "siftsmall") {
        base_file = current_path + "/../benchmarks/datasets/siftsmall/siftsmall_base.fvecs";
        query_file = current_path + "/../benchmarks//datasets/siftsmall/siftsmall_query.fvecs";
        groundtruth_file = current_path + "/../benchmarks//datasets/siftsmall/siftsmall_groundtruth.ivecs";
    } else if (FLAGS_dataset_type == "sift") {
        base_file = current_path + "/../benchmarks//datasets/sift/sift_base.fvecs";
        query_file = current_path + "/../benchmarks//datasets/sift/sift_query.fvecs";
        groundtruth_file = current_path + "/../benchmarks/datasets/sift/sift_groundtruth.ivecs";
    } else if (FLAGS_dataset_type == "gist") {
        base_file = current_path + "/../benchmarks/datasets/gist/gist_base.fvecs";
        query_file = current_path + "/../benchmarks/datasets/gist/gist_query.fvecs";
        groundtruth_file = current_path + "/../benchmarks/datasets/gist/gist_groundtruth.ivecs";
    } else {
        std::cerr << "Invalid dataset type: " << FLAGS_dataset_type << std::endl;
        return 1;
    }


    // ベースデータセットを読み込む
    std::vector<std::vector<float>> base_vectors = readFvecs(base_file);
    if (base_vectors.empty()) {
        std::cerr << "Failed to read base vectors from " << base_file << std::endl;
        return 1;
    }

    // クエリデータセットを読み込む
    std::vector<std::vector<float>> query_vectors = readFvecs(query_file);
    if (query_vectors.empty()) {
        std::cerr << "Failed to read query vectors from " << query_file << std::endl;
        return 1;
    }

    // グラウンドトゥルースを読み込む
    std::vector<std::vector<int>> groundtruth = readIvecs(groundtruth_file);
    if (groundtruth.empty()) {
        std::cerr << "Failed to read groundtruth from " << groundtruth_file << std::endl;
        return 1;
    }

    // base_vectorsの件数を表示
    std::cout << "Base vectors: " << base_vectors.size() << std::endl;

    int dimension = base_vectors[0].size();
    Campus::DistanceType distance_type = Campus::L2; // または Campus::Angular
    Campus campus(dimension, FLAGS_posting_limit, FLAGS_connection_limit, distance_type, sizeof(float));

    for (int i = 0; i < FLAGS_initial_num; ++i) {
        CampusInsertExecutor insert_executor(&campus, static_cast<const void*>(base_vectors[i].data()), i);
        insert_executor.insert();
    }
    campus.deleteAllArchivedNodes();
    int initial_node_num = campus.getNodeNum();

    bool start_flag = false;
    std::vector<int> readys;
    for (size_t i = 0; i < FLAGS_insert_threads; ++i)
    {
        readys.emplace_back(0);
    }

    std::vector<std::thread> threads;
    int vectors_per_thread = (base_vectors.size() - FLAGS_initial_num) / FLAGS_insert_threads;
    for (int i = 0; i < FLAGS_insert_threads; ++i) {
        int start = FLAGS_initial_num + i * vectors_per_thread;
        int end = (i == FLAGS_insert_threads - 1) ? base_vectors.size() : FLAGS_initial_num + (i + 1) * vectors_per_thread;
        threads.emplace_back(insertVectors, i, std::ref(readys[i]), std::ref(start_flag), &campus, std::ref(base_vectors), start, end);
    }

    while (true)
    {
        bool failed = false;
        for (auto &re : readys)
        {
            if (!__atomic_load_n(&re, __ATOMIC_SEQ_CST))
            {
                failed = true;
                break;
            }
        }
        if (!failed)
        {
            break;
        }
    }
    __atomic_store_n(&start_flag, true, __ATOMIC_SEQ_CST);

    // スループット性能とレイテンシを計測
    auto start_time = std::chrono::high_resolution_clock::now();

    for (auto &thread : threads) {
        thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;

    std::cout << "Inserted " << campus.countAllVectors() << " vectors using " << FLAGS_insert_threads << " threads in "
              << elapsed.count() << " seconds.\n";
    std::cout << "Throughput: " << base_vectors.size() / elapsed.count() << " vectors/second\n";
    std::cout << "Latency: " << elapsed.count() / base_vectors.size() << " seconds/vector\n";

    std::cout << "All vectors: " << campus.countAllVectors() << ": lost vectors: " << campus.countLostVectors() << std::endl;
    std::cout << "All vectors: " << campus.countAllVectors() << ": viloate vectors: " << campus.countViolateVectors(new L2Distance()) << std::endl;

    ofs.open(output_file, std::ios::app);
    ofs << initial_node_num << "," << base_vectors.size() / elapsed.count() << "," << elapsed.count() / base_vectors.size() << ","
        << campus.countAllVectors() << "," << campus.countUniqueVectors() << "," << campus.countViolateVectors(new L2Distance()) << ",";
    ofs.flush();
    ofs.close();

    // campus.verifyClusterAssignments(new L2Distance());
    if (FLAGS_delete_archived) {
        campus.deleteAllArchivedNodes();
    }

    readys.clear();
    readys.resize(FLAGS_search_threads);
    for (size_t i = 0; i < FLAGS_search_threads; ++i) { readys[i] = 0; }
    __atomic_store_n(&start_flag, false, __ATOMIC_SEQ_CST);

    // 類似ベクトル検索をマルチスレッドで行う
    std::vector<std::vector<int>> results(query_vectors.size());
    start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> search_threads;
    int queries_per_thread = query_vectors.size() / FLAGS_search_threads;
    for (int i = 0; i < FLAGS_search_threads; ++i) {
        int start = i * queries_per_thread;
        int end = (i == FLAGS_search_threads - 1) ? query_vectors.size() : (i + 1) * queries_per_thread;
        search_threads.emplace_back(searchVectors, i, std::ref(readys[i]), std::ref(start_flag), &campus, std::ref(query_vectors), start, end, 100, std::ref(results));
    }
    
    while (true)
    {
        bool failed = false;
        for (auto &re : readys)
        {
            if (!__atomic_load_n(&re, __ATOMIC_SEQ_CST))
            {
                failed = true;
                break;
            }
        }
        if (!failed)
        {
            break;
        }
    }
    __atomic_store_n(&start_flag, true, __ATOMIC_SEQ_CST);
    start_time = std::chrono::high_resolution_clock::now();

    for (auto &thread : search_threads) {
        thread.join();
    }

    end_time = std::chrono::high_resolution_clock::now();
    elapsed = end_time - start_time;

    std::cout << "Searched " << query_vectors.size() << " queries using " << FLAGS_search_threads << " threads in "
              << elapsed.count() << " seconds.\n";
    std::cout << "Throughput: " << query_vectors.size() / elapsed.count() << " queries/second\n";
    std::cout << "Latency: " << elapsed.count() / query_vectors.size() << " seconds/query\n";

    // リコールを計算
    float recall = calculateRecall(results, groundtruth);
    std::cout << "Recall: " << recall << std::endl;

    ofs.open(output_file, std::ios::app);
    ofs << query_vectors.size() / elapsed.count() << "," << elapsed.count() / query_vectors.size() << "," << recall << "\n";
    ofs.flush();
    ofs.close();

    return 0;
}
