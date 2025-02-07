#ifndef NOCONTROL_H
#define NOCONTROL_H

#include "node.h"
#include "../utils/distance.h"
#include <cassert>
#include <vector>
#include <mutex>
#include <memory>
#include <algorithm>

class NoControl {
public:
    enum DistanceType {
        L2,
        Angular
    };

    NoControl(int dimension, int posting_limit, int connection_limit, DistanceType distance_type, size_t element_size)
        : dimension_(dimension), posting_limit_(posting_limit), connection_limit_(connection_limit), distance_type_(distance_type), element_size_(element_size), node_num_(0) {
        entry_point_ = new Node(10, 10, sizeof(float));
    }

    ~NoControl() {
        delete entry_point_;
    }

    Node *getEntryPoint() const { return entry_point_; }
    int getNodeNum() const { return node_num_; }
    void setEntryPoint(Node *node) { entry_point_ = node; }
    int getDimension() const { return dimension_; }
    int getPostingLimit() const { return posting_limit_; }
    int getConnectionLimit() const { return connection_limit_; }
    Node *findExactNearestNode(const void *query_vector, Distance *distance);
    std::vector<Node*> findNearestNodes(const void *query_vector, Distance *distance, int n);
    DistanceType getDistanceType() const { return distance_type_; }
    void incrementNodeNum() { node_num_++; }
    void addNode(Node *node) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto new_nodes = std::make_shared<std::vector<Node*>>(*all_nodes_);
        new_nodes->push_back(node);
        all_nodes_ = new_nodes;
    }
    void deleteNode(Node *node) {
        auto new_nodes = std::make_shared<std::vector<Node*>>(*all_nodes_);
        new_nodes->erase(std::remove(new_nodes->begin(), new_nodes->end(), node), new_nodes->end());
        all_nodes_ = new_nodes;
    }

private:
    DistanceType distance_type_;
    int dimension_;
    int posting_limit_;
    int connection_limit_;
    size_t element_size_;
    Node *entry_point_;
    int node_num_;
    std::mutex mutex_;
    std::shared_ptr<std::vector<Node*>> all_nodes_ = std::make_shared<std::vector<Node*>>();
};

class NoControlInsertExecutor {
public:
    NoControlInsertExecutor(NoControl *nocontrol, const void *insert_vector, int vector_id)
        : nocontrol_(nocontrol), insert_vector_(insert_vector), vector_id_(vector_id) {
            switch (nocontrol_->getDistanceType()) {
            case NoControl::L2:
                distance_ = new L2Distance();
                break;
            case NoControl::Angular:
                distance_ = new AngularDistance();
                break;
            }
        }

    void insert();

private:
    Distance *distance_;
    NoControl *nocontrol_;
    const void *insert_vector_;
    const int vector_id_;
    void split(Node *spliting_node, const void *insert_vector, int vector_id);
    void assign(Node *new_node1, Node *new_node2);
    void reassign(Node *spliting_node, Node *new_node1, Node *new_node2);
    void connectNeighbors(Node *spliting_node, Node *new_node1, Node *new_node2, int connection_limit);
    void updateNeighbors(Node *spliting_node, Node *new_node1, Node *new_node2, int connection_limit);
};

#endif // NOCONTROL_H
