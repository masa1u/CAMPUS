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

std::vector<Node*> Campus::findExactNearestNodes(const void *query_vector, Distance *distance, int n) {
    std::priority_queue<std::pair<float, Node*>> pq;
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

        if (pq.size() < n) {
            pq.push(std::make_pair(current_distance, node));
        } else if (current_distance < pq.top().first) {
            pq.pop();
            pq.push(std::make_pair(current_distance, node));
        }
    }
    std::vector<Node*> result;
    while (!pq.empty()) {
        result.push_back(pq.top().second);
        pq.pop();
    }
    return result;
}


std::vector<Node*> Campus::findNearestNodes(const void *query_vector, Distance *distance, int node_num, int pq_size) {
    using NodeDistance = std::pair<float, Node*>;
    std::vector<NodeDistance> search_candidates;
    // min heap for result priority queue
    auto cmp = [](NodeDistance left, NodeDistance right) { return left.first > right.first; };
    std::priority_queue<NodeDistance, std::vector<NodeDistance>, decltype(cmp)> result_pq(cmp);
    std::unordered_set<Node*> visited;

    if (entry_point_ == nullptr) return {};

    float distance_to_entry = distance->calculateDistance(static_cast<const float*>(entry_point_->getLatestVersion()->getCentroid()), static_cast<const float*>(query_vector), dimension_);
    search_candidates.push_back(std::make_pair(distance_to_entry, entry_point_));

    while (!search_candidates.empty()) {
        NodeDistance current = search_candidates[0];
        search_candidates.erase(search_candidates.begin());

        if (visited.find(current.second) != visited.end()) {
            continue;
        }

        visited.insert(current.second);
        result_pq.push(current);


        for (Node *neighbor : current.second->getLatestVersion()->getOutNeighbors()) {
            float distance_to_neighbor = distance->calculateDistance(static_cast<const float*>(neighbor->getLatestVersion()->getCentroid()), static_cast<const float*>(query_vector), dimension_);
            if (visited.find(neighbor) != visited.end()) {
                continue;
            }
            // insert neighbor to search_candidates which is sorted by distance
            if (search_candidates.empty()) {
                search_candidates.push_back(std::make_pair(distance_to_neighbor, neighbor));
            } else {
                bool inserted = false;
                for (int i = 0; i < search_candidates.size(); ++i) {
                    if (distance_to_neighbor < search_candidates[i].first) {
                        search_candidates.insert(search_candidates.begin() + i, std::make_pair(distance_to_neighbor, neighbor));
                        inserted = true;
                        break;
                    }
                }
                if (!inserted) {
                    search_candidates.push_back(std::make_pair(distance_to_neighbor, neighbor));
                }
            }

            if (search_candidates.size() > pq_size) {
                search_candidates.pop_back(); // 最も類似していない要素を削除
            }
        }
    }

    std::vector<Node*> result;
    while (!result_pq.empty() && result.size() < node_num) {
        result.push_back(result_pq.top().second);
        result_pq.pop();
    }
    return result;
}



std::vector<int> Campus::topKSearch(const void *query_vector, int top_k, Distance *distance, int node_num, int pq_size) {
    std::vector<Node*> nearest_nodes = findNearestNodes(query_vector, distance, node_num, pq_size);
    // std::vector<Node*> nearest_nodes = findExactNearestNodes(query_vector, distance, node_num);
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
