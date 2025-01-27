#include "../src/nocontrol/nocontrol.h"
#include <iostream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include <algorithm>
#include <thread>
#include <chrono>
// #include <hdf5.h>

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


// 乱数でデータセットを生成する関数
std::vector<std::vector<float>> generateRandomDataset(size_t num_vectors, size_t dimension) {
    std::vector<std::vector<float>> dataset(num_vectors, std::vector<float>(dimension));
    std::srand(static_cast<unsigned int>(std::time(nullptr)));

    for (auto &vec : dataset) {
        for (auto &value : vec) {
            value = static_cast<float>(std::rand()) / RAND_MAX;
        }
    }

    // データセットを正規化
    normalizeDataset(dataset);

    return dataset;
}

// ベクトルを挿入する関数
void insertVectors(NoControl *nocontrol, const std::vector<std::vector<float>> &vectors, int start, int end) {
    for (int i = start; i < end; ++i) {
        NoControlInsertExecutor insert_executor(nocontrol, static_cast<const void*>(vectors[i].data()), i);
        insert_executor.insert();
    }
}


int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <num_threads> <dataset_path>" << std::endl;
        return 1;
    }

    int num_threads = std::stoi(argv[1]);
    // std::string dataset_path = argv[2];

    NoControl::DistanceType distance_type = NoControl::L2; // または Campus::Angular
    NoControl nocontrol(128, 20, 10, distance_type, sizeof(float));

    // データセットを生成（L2正規化を含む）
    // std::vector<std::vector<float>> vectors = loadDataset(dataset_path, "/train");
    std::vector<std::vector<float>> vectors = generateRandomDataset(1000, 128);

    if (vectors.size() == 0 || vectors[0].size() != 128) {
        std::cerr << "Invalid dataset dimensions or empty dataset." << std::endl;
        return 1;
    }

    // 先頭の1000件だけを使う
    if (vectors.size() > 1000) {
        vectors.resize(1000);
    }

    // スループット性能とレイテンシを計測
    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    int vectors_per_thread = vectors.size() / num_threads;
    for (int i = 0; i < num_threads; ++i) {
        int start = i * vectors_per_thread;
        int end = (i == num_threads - 1) ? vectors.size() : (i + 1) * vectors_per_thread;
        threads.emplace_back(insertVectors, &nocontrol, std::ref(vectors), start, end);
    }

    for (auto &thread : threads) {
        thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;

    std::cout << "Inserted " << vectors.size() << " vectors using " << num_threads << " threads in "
              << elapsed.count() << " seconds.\n";
    std::cout << "Throughput: " << vectors.size() / elapsed.count() << " vectors/second\n";
    std::cout << "Latency: " << elapsed.count() / vectors.size() << " seconds/vector\n";
    std::cout << "Node num: " << nocontrol.getNodeNum() << std::endl;

    return 0;
}