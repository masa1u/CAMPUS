#include "nocontrol.h"
#include <queue>
#include <unordered_set>
#include <cassert>

Node *NoControl::findExactNearestNode(const void *query_vector, Distance *distance) {
    Node *nearest_node = nullptr;
    float min_distance = std::numeric_limits<float>::max();

    std::shared_ptr<std::vector<Node*>> nodes_snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        nodes_snapshot = all_nodes_;
    }

    for (Node *node : *nodes_snapshot) {
        float current_distance = distance->calculateDistance(static_cast<const float*>(node->getCentroid()), static_cast<const float*>(query_vector), dimension_);

        if (current_distance < min_distance) {
            min_distance = current_distance;
            nearest_node = node;
        }
    }
    return nearest_node;
}

std::vector<Node*> NoControl::findNearestNodes(const void *query_vector, Distance *distance, int n) {
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

        float current_distance = distance->calculateDistance(static_cast<const float*>(current_node->getCentroid()), static_cast<const float*>(query_vector), dimension_);

        if (pq.size() < n) {
            pq.push(std::make_pair(current_distance, current_node));
        } else if (current_distance < pq.top().first) {
            pq.pop();
            pq.push(std::make_pair(current_distance, current_node));
        }

        // Explore neighbors
        for (Node* neighbor_node : current_node->getOutNeighbors()) {
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