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

#include "../faiss/faiss/IndexFlat.h"
#include "../faiss/faiss/IndexIVFFlat.h"

using idx_t = faiss::idx_t;


DEFINE_string(dataset_type, "siftsmall", "Dataset type (siftsmall, sift, gist)");
// parameters for index itself
DEFINE_int32(nlist, 100, "Number of nodes in IVF");

// parameters for search operation
DEFINE_int32(search_threads, 1, "Number of threads for search");
DEFINE_int32(top_k, 100, "Number of top k elements to search");
DEFINE_int32(node_num, 10, "nprobe, number of nodes to search");

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
    ofs << "ivfflat," << FLAGS_dataset_type << ",0," << FLAGS_nlist << ",nill,1,1,null," << FLAGS_top_k << "," << FLAGS_node_num << ",null,";
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
    std::vector<std::vector<float>> base_vectors_2d = readFvecs(base_file);
    if (base_vectors_2d.empty()) {
        std::cerr << "Failed to read base vectors from " << base_file << std::endl;
        return 1;
    }

    // クエリデータセットを読み込む
    std::vector<std::vector<float>> query_vectors_2d = readFvecs(query_file);
    if (query_vectors_2d.empty()) {
        std::cerr << "Failed to read query vectors from " << query_file << std::endl;
        return 1;
    }

    // グラウンドトゥルースを読み込む
    std::vector<std::vector<int>> groundtruth = readIvecs(groundtruth_file);
    if (groundtruth.empty()) {
        std::cerr << "Failed to read groundtruth from " << groundtruth_file << std::endl;
        return 1;
    }

    // 2Dベクトルを1Dベクトルに変換
    int dimension = base_vectors_2d[0].size();
    int nb = base_vectors_2d.size();
    int nq = query_vectors_2d.size();

    float* xb = new float[dimension * nb];
    float* xq = new float[dimension * nq];

    for (int i = 0; i < nb; i++) {
        for (int j = 0; j < dimension; j++) {
            xb[dimension * i + j] = base_vectors_2d[i][j];
        }
    }

    for (int i = 0; i < nq; i++) {
        for (int j = 0; j < dimension; j++) {
            xq[dimension * i + j] = query_vectors_2d[i][j];
        }
    }


    // base_vectorsの件数を表示
    std::cout << "Base vectors: " << nb << std::endl;

    faiss::IndexFlatL2 quantizer(dimension); // the other index
    faiss::IndexIVFFlat index(&quantizer, dimension, FLAGS_nlist);

    // スループット性能とレイテンシを計測
    auto start_time = std::chrono::high_resolution_clock::now();

    index.train(nb, xb);
    index.add(nb, xb);

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;

    std::cout << "Inserted " << nb << " vectors in "
              << elapsed.count() << " seconds.\n";
    std::cout << "Throughput: " << nb / elapsed.count() << " vectors/second\n";
    std::cout << "Latency: " << elapsed.count() / nb << " seconds/vector\n";

    ofs.open(FLAGS_output_file, std::ios::app);
    ofs << "0," << nb / elapsed.count() << "," << elapsed.count() / nb << ","
        << nb << "," << nb << "," << 0 << ",";
    ofs.flush();
    ofs.close();

    // 類似ベクトル検索を行う
    int k = FLAGS_top_k;
    idx_t* I = new idx_t[k * nq];
    float* D = new float[k * nq];

    start_time = std::chrono::high_resolution_clock::now();

    index.search(nq, xq, k, D, I);

    end_time = std::chrono::high_resolution_clock::now();
    elapsed = end_time - start_time;

    // リコールを計算するために結果を整形
    std::vector<std::vector<int>> results(nq);
    for (int i = 0; i < nq; i++) {
        for (int j = 0; j < k; j++) {
            results[i].push_back(I[i * k + j]);
        }
    }
    float recall = calculateRecall(results, groundtruth);


    std::cout << "Searched " << nq << " queries in "
              << elapsed.count() << " seconds.\n";
    std::cout << "Throughput: " << nq / elapsed.count() << " queries/second\n";
    std::cout << "Latency: " << elapsed.count() / nq << " seconds/query\n";
    std::cout << "Recall: " << recall << std::endl;

    delete[] xb;
    delete[] xq;
    delete[] I;
    delete[] D;

    return 0;
    return 0;
}
