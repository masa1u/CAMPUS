#include "serial.h"
#include <queue>
#include <vector>
#include <limits>
#include <algorithm>
#include <iostream>
#include <unordered_set>


Node *Serial::findExactNearestNode(const void *query_vector, Distance *distance) {
    Node *nearest_node = nullptr;
    float min_distance = std::numeric_limits<float>::max();

    for (Node *node : all_nodes_) {
        float current_distance = distance->calculateDistance(static_cast<const float*>(node->getCentroid()), static_cast<const float*>(query_vector), dimension_);

        if (current_distance < min_distance) {
            min_distance = current_distance;
            nearest_node = node;
        }
    }

    return nearest_node;
}

std::vector<Node*> Serial::findExactNearestNodes(const void *query_vector, Distance *distance, int n) {
    std::priority_queue<std::pair<float, Node*>> pq;
    float min_distance = std::numeric_limits<float>::max();

    for (Node *node : all_nodes_) {
        assert(node != nullptr);

        float current_distance = distance->calculateDistance(static_cast<const float*>(node->getCentroid()), static_cast<const float*>(query_vector), dimension_);

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


std::vector<Node*> Serial::findNearestNodes(const void *query_vector, Distance *distance, int node_num, int pq_size) {
    using NodeDistance = std::pair<float, Node*>;
    std::vector<NodeDistance> search_candidates;
    std::unordered_set<Node*> visited;

    if (entry_point_ == nullptr) return {};
    float distance_to_entry = distance->calculateDistance(static_cast<const float*>(entry_point_->getCentroid()), static_cast<const float*>(query_vector), dimension_);
    search_candidates.push_back(std::make_pair(distance_to_entry, entry_point_));

    while(true) {
        bool updated = false;
        for (NodeDistance current : search_candidates) {
            if (visited.find(current.second) != visited.end()) {
                continue;
            }
            visited.insert(current.second);

            for (Node *neighbor : current.second->getOutNeighbors()) {
                assert(neighbor != nullptr);
                float distance_to_neighbor = distance->calculateDistance(static_cast<const float*>(neighbor->getCentroid()), static_cast<const float*>(query_vector), dimension_);
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
                updated = true;
            }
            if(updated) break;  
        }
        if (!updated) {
            break;
        }
    }
    std::vector<Node*> result;
    for (int i = 0; i < node_num && i < node_num; ++i) {
        result.push_back(search_candidates[i].second);
    }
    return result;
}


std::vector<int> Serial::topKSearch(const void *query_vector, int top_k, Distance *distance, int node_num, int pq_size) {
    // std::vector<Node*> nearest_nodes = findNearestNodes(query_vector, distance, node_num, pq_size);
    std::vector<Node*> nearest_nodes = findExactNearestNodes(query_vector, distance, node_num);
    std::priority_queue<std::pair<float, int>> pq;

    for (Node *node : nearest_nodes) {
        Entity **posting = node->getPosting();
        for (int i = 0; i < node->getVectorNum(); ++i) {
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



bool Serial::verifyClusterAssignments(Distance *distance) {
    int viloation_count = 0;

    for (Node *node : all_nodes_) {

        Entity **posting = node->getPosting();
        for (int i = 0; i < node->getVectorNum(); ++i) {
            float assigned_distance = distance->calculateDistance(static_cast<const float*>(posting[i]->getVector()), static_cast<const float*>(node->getCentroid()), dimension_);
            float min_distance = std::numeric_limits<float>::max();
            for (Node *other_node : all_nodes_){
                if (node == other_node) {
                    continue;
                }
                float compared_distance = distance->calculateDistance(static_cast<const float*>(posting[i]->getVector()), static_cast<const float*>(other_node->getCentroid()), dimension_);
                if (assigned_distance > compared_distance) {
                    if (min_distance > compared_distance) {
                        min_distance = compared_distance;
                    }
                }
            }
            if (min_distance < assigned_distance) {
                std::cout << "[Viloation detected] Vec" << posting[i]->id << " " << assigned_distance << " " << min_distance << std::endl;
                viloation_count++;
            }
        }
    }
    std::cout << "Viloation count " << viloation_count << std::endl;
    return true;
}
