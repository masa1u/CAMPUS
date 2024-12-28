#include "campus.h"
#include <queue>
#include <vector>
#include <limits>
#include <algorithm>
#include <iostream>


Node *Campus::findExactNearestNode(const void *query_vector, Distance *distance) {
    Node *nearest_node = nullptr;
    float min_distance = std::numeric_limits<float>::max();
    std::vector<bool> visited(node_size_, false);

    std::queue<Node*> node_queue;
    node_queue.push(entry_point_);

    while (!node_queue.empty()) {
        Node *current_node = node_queue.front();
        node_queue.pop();

        if (visited[current_node->getNodeId()]) {
            continue;
        }
        visited[current_node->getNodeId()] = true;

        Version *latest_version = current_node->getLatestVersion();
        float distance_to_query = distance->calculateDistance(static_cast<const float*>(latest_version->getCentroid()), static_cast<const float*>(query_vector), dimension_);
        if (distance_to_query < min_distance) {
            min_distance = distance_to_query;
            nearest_node = current_node;
        }

        for (int neighbor_id : current_node->getNeighbors()) {
            if (!visited[neighbor_id]) {
                Node *neighbor_node = link_list_[neighbor_id];
                if (neighbor_node != nullptr) {
                    node_queue.push(neighbor_node);
                }
            }
        }
    }

    return nearest_node;
}


std::vector<Node*> Campus::findNearestNodes(const void *query_vector, Distance *distance, int n) {
    std::priority_queue<std::pair<float, Node*>> pq;
    std::vector<Node*> result;
    std::vector<bool> visited(node_size_, false);

    if (entry_point_ == nullptr) {
        return result;
    }

    pq.push(std::make_pair(0.0f, entry_point_));

    while (!pq.empty()) {
        Node *current_node = pq.top().second;
        pq.pop();

        if (visited[current_node->getNodeId()]) {
            continue;
        }
        visited[current_node->getNodeId()] = true;

        Version *latest_version = current_node->getLatestVersion();
        float current_distance = distance->calculateDistance(static_cast<const float*>(latest_version->getCentroid()), static_cast<const float*>(query_vector), dimension_);
        result.push_back(current_node);
        if (result.size() >= n) {
            break;
        }

        // Explore neighbors
        for (int neighbor_id : current_node->getNeighbors()) {
            if (!visited[neighbor_id]) {
                Node *neighbor_node = link_list_[neighbor_id];
                if (neighbor_node != nullptr) {
                    Version *neighbor_version = neighbor_node->getLatestVersion();
                    float neighbor_distance = distance->calculateDistance(static_cast<const float*>(neighbor_version->getCentroid()), static_cast<const float*>(query_vector), dimension_);
                    pq.push(std::make_pair(neighbor_distance, neighbor_node));
                }
            }
        }
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
    std::vector<bool> visited(node_size_, false);

    std::queue<Node*> node_queue;
    node_queue.push(entry_point_);

    while (!node_queue.empty()) {
        Node *current_node = node_queue.front();
        node_queue.pop();

        if (visited[current_node->getNodeId()]) {
            continue;
        }
        visited[current_node->getNodeId()] = true;

        Version *latest_version = current_node->getLatestVersion();
        for (Entity *entity : latest_version->getPosting()) {
            all_ids.push_back(entity->id);
        }

        for (int neighbor_id : current_node->getNeighbors()) {
            if (!visited[neighbor_id]) {
                Node *neighbor_node = link_list_[neighbor_id];
                if (neighbor_node != nullptr) {
                    node_queue.push(neighbor_node);
                }
            }
        }
    }

    return all_ids;
}

void Campus::addNodeToGraph(Node *new_node) {
    if (link_list_ == nullptr) {
        link_list_ = new Node*[posting_limit_];
        for (int i = 0; i < posting_limit_; ++i) {
            link_list_[i] = nullptr;
        }
    }

    for (int i = 0; i < posting_limit_; ++i) {
        if (link_list_[i] == nullptr) {
            link_list_[i] = new_node;
            break;
        }
    }
    entry_point_ = new_node;
    node_size_++;
}