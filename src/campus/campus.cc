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
    std::unordered_set<Node*> visited;

    std::queue<Node*> node_queue;
    node_queue.push(entry_point_);

    while (!node_queue.empty()) {
        Node *current_node = node_queue.front();
        assert(current_node->isArchived() == false);
        node_queue.pop();

        if (visited.find(current_node) != visited.end()) {
            continue;
        }
        visited.insert(current_node);

        Version *latest_version = current_node->getLatestVersion();
        float distance_to_query = distance->calculateDistance(static_cast<const float*>(latest_version->getCentroid()), static_cast<const float*>(query_vector), dimension_);
        if (distance_to_query < min_distance) {
            min_distance = distance_to_query;
            nearest_node = current_node;
        }

        for (Node* neighbor_node : latest_version->getOutNeighbors()) {
            if (visited.find(neighbor_node) == visited.end()) {
                node_queue.push(neighbor_node);
            }
        }
    }

    return nearest_node;
}

std::vector<Node*> Campus::findNearestNodes(const void *query_vector, Distance *distance, int n) {
    std::priority_queue<std::pair<float, Node*>> pq;
    std::unordered_set<Node*> visited;

    if (entry_point_ == nullptr) {
        return {};
    }

    pq.push(std::make_pair(0.0f, entry_point_));

    while (!pq.empty()) {
        Node *current_node = pq.top().second;
        pq.pop();

        if (visited.find(current_node) != visited.end()) {
            continue;
        }
        visited.insert(current_node);

        Version *latest_version = current_node->getLatestVersion();
        float current_distance = distance->calculateDistance(static_cast<const float*>(latest_version->getCentroid()), static_cast<const float*>(query_vector), dimension_);

        if (pq.size() < n) {
            pq.push(std::make_pair(current_distance, current_node));
        } else if (current_distance < pq.top().first) {
            pq.pop();
            pq.push(std::make_pair(current_distance, current_node));
        }

        // Explore neighbors
        for (Node* neighbor_node : latest_version->getOutNeighbors()) {
            if (visited.find(neighbor_node) == visited.end()) {
                pq.push(std::make_pair(current_distance, neighbor_node));
            }
        }
    }

    std::vector<Node*> result;
    while (!pq.empty()) {
        result.push_back(pq.top().second);
        pq.pop();
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