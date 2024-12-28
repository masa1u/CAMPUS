#include "../src/campus/campus.h"
#include <iostream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include <algorithm>

// ブルートフォースで類似しているベクトルを検索する関数
std::vector<int> bruteForceSearch(const std::vector<std::vector<float>>& vectors, const float* query_vector, int top_k) {
    std::vector<std::pair<float, int>> distances;
    for (int i = 0; i < vectors.size(); ++i) {
        float distance = 0.0;
        for (int j = 0; j < 128; ++j) {
            float diff = vectors[i][j] - query_vector[j];
            distance += diff * diff;
        }
        distances.push_back(std::make_pair(std::sqrt(distance), i));
    }
    std::sort(distances.begin(), distances.end());
    std::vector<int> result;
    for (int i = 0; i < top_k; ++i) {
        result.push_back(distances[i].second);
    }
    return result;
}

int main() {
    Campus::DistanceType distance_type = Campus::L2; // または Campus::Angular
    Campus campus(128, 1000, distance_type, sizeof(float));

    // Insert 100 vectors
    std::srand(std::time(nullptr));
    std::vector<std::vector<float>> vectors;
    for (int i = 0; i < 100; ++i) {
        std::vector<float> insert_vector(128);
        for (int j = 0; j < 128; ++j) {
            insert_vector[j] = static_cast<float>(std::rand()) / RAND_MAX; // Generate a random float between 0 and 1
        }
        vectors.push_back(insert_vector);
        CampusInsertExecutor insert_executor(&campus, static_cast<void*>(insert_vector.data()), i);
        insert_executor.insert();
    }

    std::cout << "Inserted 100 vectors\n";

    // Perform a similarity search
    float query_vector[128];
    for (int j = 0; j < 128; ++j) {
        query_vector[j] = static_cast<float>(std::rand()) / RAND_MAX; // Generate a random float between 0 and 1
    }
    CampusQueryExecutor query_executor(&campus, static_cast<void*>(query_vector), 10); // top 10 similar vectors
    std::vector<int> result = query_executor.query();

    // Print the results
    std::cout << "Top 10 similar vectors to query_vector (Campus):\n";
    for (int id : result) {
        std::cout << "Vector ID: " << id << "\n";
    }

    // ブルートフォースで類似しているベクトルを検索
    std::vector<int> brute_force_result = bruteForceSearch(vectors, query_vector, 10);

    // Print the brute force results
    std::cout << "Top 10 similar vectors to query_vector (Brute Force):\n";
    for (int id : brute_force_result) {
        std::cout << "Vector ID: " << id << "\n";
    }

    return 0;
}