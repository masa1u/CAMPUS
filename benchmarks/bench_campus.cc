#include "../src/campus/campus.h"
#include <iostream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include <algorithm>
#include <thread>
#include <chrono>
#include <hdf5.h>

// HDF5ファイルからデータセットを読み込む関数
std::vector<std::vector<float>> loadDataset(const std::string &file_path, const std::string &dataset_name) {
    std::vector<std::vector<float>> dataset;
    hid_t file_id = H5Fopen(file_path.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    if (file_id < 0) {
        std::cerr << "Error opening file: " << file_path << std::endl;
        return dataset;
    }

    hid_t dataset_id = H5Dopen(file_id, dataset_name.c_str(), H5P_DEFAULT);
    if (dataset_id < 0) {
        std::cerr << "Error opening dataset: " << dataset_name << std::endl;
        H5Fclose(file_id);
        return dataset;
    }

    hid_t dataspace_id = H5Dget_space(dataset_id);
    if (dataspace_id < 0) {
        std::cerr << "Error getting dataspace for dataset: " << dataset_name << std::endl;
        H5Dclose(dataset_id);
        H5Fclose(file_id);
        return dataset;
    }

    hsize_t dims[2];
    if (H5Sget_simple_extent_dims(dataspace_id, dims, nullptr) < 0) {
        std::cerr << "Error getting dataset dimensions for dataset: " << dataset_name << std::endl;
        H5Sclose(dataspace_id);
        H5Dclose(dataset_id);
        H5Fclose(file_id);
        return dataset;
    }

    dataset.resize(dims[0], std::vector<float>(dims[1]));
    if (H5Dread(dataset_id, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, dataset[0].data()) < 0) {
        std::cerr << "Error reading dataset: " << dataset_name << std::endl;
    }

    H5Sclose(dataspace_id);
    H5Dclose(dataset_id);
    H5Fclose(file_id);

    return dataset;
}

void insertVectors(Campus *campus, const std::vector<std::vector<float>> &vectors, int start, int end) {
    for (int i = start; i < end; ++i) {
        CampusInsertExecutor insert_executor(campus, static_cast<const void*>(vectors[i].data()), i);
        insert_executor.insert();
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <num_threads> <dataset_path>" << std::endl;
        return 1;
    }

    int num_threads = std::stoi(argv[1]);
    std::string dataset_path = argv[2];

    Campus::DistanceType distance_type = Campus::L2; // または Campus::Angular
    Campus campus(128, 100, 10, distance_type, sizeof(float));

    // データセットを読み込む
    std::vector<std::vector<float>> vectors = loadDataset(dataset_path, "/train");

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
        threads.emplace_back(insertVectors, &campus, std::ref(vectors), start, end);
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

    return 0;
}