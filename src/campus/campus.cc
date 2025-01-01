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

        for (Node* neighbor_node : latest_version->getNeighbors()) {
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
        for (Node* neighbor_node : latest_version->getNeighbors()) {
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
        for (Entity *entity : node->getLatestVersion()->getPosting()) {
            float entity_distance = distance->calculateDistance(static_cast<const float*>(entity->getVector()), static_cast<const float*>(query_vector), dimension_);
            pq.push(std::make_pair(entity_distance, entity->id));
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

std::vector<int> Campus::getAllVectorIds() {
    std::vector<int> all_ids;
    std::unordered_set<Node*> visited;

    std::queue<Node*> node_queue;
    node_queue.push(entry_point_);

    while (!node_queue.empty()) {
        Node *current_node = node_queue.front();
        node_queue.pop();

        if (visited.find(current_node) != visited.end()) {
            continue;
        }
        visited.insert(current_node);

        Version *latest_version = current_node->getLatestVersion();
        for (Entity *entity : latest_version->getPosting()) {
            all_ids.push_back(entity->id);
        }

        for (Version* neighbor_version : latest_version->getNeighbors()) {
            Node *neighbor_node = neighbor_version->getNode();
            if (visited.find(neighbor_node) == visited.end()) {
                node_queue.push(neighbor_node);
            }
        }
    }

    return all_ids;
}

void Campus::addNodeToGraph(Node *new_node) {
    link_list_[node_size_] = new_node;
    entry_point_ = new_node;
    node_size_++;
}

void Campus::resizeLinkList() {
    int new_capacity = (link_list_capacity_ == 0) ? 1 : link_list_capacity_ * 2;
    Node **new_link_list = static_cast<Node**>(malloc(new_capacity * sizeof(Node*)));
    for (int i = 0; i < node_size_; ++i) {
        new_link_list[i] = link_list_[i];
    }
    free(link_list_);
    link_list_ = new_link_list;
    link_list_capacity_ = new_capacity;
}

void Campus::switchVersion(Node *node, Version *new_version) {
    node->switchVersion(new_version);
    // latest_versionを更新していく作業
}