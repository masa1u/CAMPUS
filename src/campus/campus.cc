#include "campus.h"
#include <queue>
#include <vector>
#include <limits>
#include <algorithm>
#include <iostream>
#include <unordered_set>


Node *Campus::findExactNearestNode(const void *query_vector, Distance *distance) {
    Node *nearest_node = nullptr;
    float min_distance = std::numeric_limits<float>::max();

    std::shared_ptr<std::vector<Node*>> nodes_snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        nodes_snapshot = all_nodes_;
    }

    for (Node *node : *nodes_snapshot) {
        assert(node != nullptr);
        if (node->isArchived()) {
            continue;
        }
        Version *latest_version = node->getLatestVersion();
        float current_distance = distance->calculateDistance(static_cast<const float*>(latest_version->getCentroid()), static_cast<const float*>(query_vector), dimension_);

        if (current_distance < min_distance) {
            min_distance = current_distance;
            nearest_node = node;
        }
    }
    return nearest_node;
}

// std::vector<Node*> Campus::findNearestNodes(const void *query_vector, Distance *distance, int n) {
//     std::priority_queue<std::pair<float, Node*>> pq;
//     std::unordered_set<Node*> visited;

//     if (entry_point_ == nullptr) {
//         return {};
//     }

//     pq.push(std::make_pair(0.0f, entry_point_));

//     while (!pq.empty()) {
//         Node *current_node = pq.top().second;
//         pq.pop();

//         if (visited.find(current_node) != visited.end()) {
//             continue;
//         }
//         visited.insert(current_node);

//         Version *latest_version = current_node->getLatestVersion();
//         float current_distance = distance->calculateDistance(static_cast<const float*>(latest_version->getCentroid()), static_cast<const float*>(query_vector), dimension_);

//         if (pq.size() < n) {
//             pq.push(std::make_pair(current_distance, current_node));
//         } else if (current_distance < pq.top().first) {
//             pq.pop();
//             pq.push(std::make_pair(current_distance, current_node));
//         };

//         // Explore neighbors
//         for (Node* neighbor_node : latest_version->getOutNeighbors()) {
//             if (visited.find(neighbor_node) == visited.end()) {
//                 pq.push(std::make_pair(current_distance, neighbor_node));
//             }
//         }
//     }

//     std::vector<Node*> result;
//     while (!pq.empty()) {
//         result.push_back(pq.top().second);
//         pq.pop();
//     }

//     std::reverse(result.begin(), result.end());
//     return result;
// }

std::vector<Node*> Campus::findNearestNodes(const void *query_vector, Distance *distance, int n) {
    auto compare_min = [](const std::pair<float, Node*>& lhs, const std::pair<float, Node*>& rhs) {
        return lhs.first > rhs.first;
    };
    auto compare_max = [](const std::pair<float, Node*>& lhs, const std::pair<float, Node*>& rhs) {
        return lhs.first < rhs.first;
    };

    std::priority_queue<std::pair<float, Node*>, std::vector<std::pair<float, Node*>>, decltype(compare_min)> C(compare_min);
    std::priority_queue<std::pair<float, Node*>, std::vector<std::pair<float, Node*>>, decltype(compare_max)> W(compare_max);
    std::unordered_set<Node*> visited;

    if (entry_point_ == nullptr) {
        return {};
    }

    float initial_distance = distance->calculateDistance(static_cast<const float*>(entry_point_->getLatestVersion()->getCentroid()),
        static_cast<const float*>(query_vector), dimension_);
    C.push(std::make_pair(initial_distance, entry_point_));
    W.push(std::make_pair(initial_distance, entry_point_));
    visited.insert(entry_point_);

    while (!C.empty()) {
        Node *current_node = C.top().second;
        float current_distance = C.top().first;
        C.pop();

        if (current_distance > W.top().first && W.size() == n) {
            break;
        }

        Version *latest_version = current_node->getLatestVersion();
        for (Node* neighbor_node : latest_version->getOutNeighbors()) {
            if (visited.find(neighbor_node) == visited.end()) {
                visited.insert(neighbor_node);
                float neighbor_distance = distance->calculateDistance(static_cast<const float*>(neighbor_node->getLatestVersion()->getCentroid()),
                    static_cast<const float*>(query_vector), dimension_);
                C.push(std::make_pair(neighbor_distance, neighbor_node));
                W.push(std::make_pair(neighbor_distance, neighbor_node));
                if (W.size() > n) {
                    W.pop();
                }
            }
        }
    }

    std::vector<Node*> result;
    while (!W.empty()) {
        result.push_back(W.top().second);
        W.pop();
    }

    std::reverse(result.begin(), result.end());
    return result;
}

std::vector<int> Campus::topKSearch(const void *query_vector, int top_k, Distance *distance, int n) {
    std::vector<Node*> nearest_nodes = findNearestNodes(query_vector, distance, n);
    std::priority_queue<std::pair<float, int>> pq;

    for (Node *node : nearest_nodes) {
        Entity **posting = node->getLatestVersion()->getPosting();
        for (int i = 0; i < node->getLatestVersion()->getVectorNum(); ++i) {
            float entity_distance = distance->calculateDistance(static_cast<const void*>(posting[i]->getVector()), static_cast<const void*>(query_vector), dimension_);
            pq.push(std::make_pair(entity_distance, posting[i]->id));
            if (pq.size() > top_k) {
                pq.pop();
            }
        }
    }

    std::vector<int> result;
    while (!pq.empty()) {
        result.push_back(pq.top().second);
        pq.pop();
    }

    std::reverse(result.begin(), result.end());
    return result;
}


void Campus::switchVersion(Node *node, Version *new_version) {
    node->switchVersion(new_version);
}