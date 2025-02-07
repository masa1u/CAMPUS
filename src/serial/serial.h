#ifndef SERIAL_H
#define SERIAL_H

#include "node.h"
#include "../utils/distance.h"
#include "../utils/lock.h"
#include <vector>
#include <unordered_set>

class Serial {
public:
    enum DistanceType {
        L2,
        Angular
    };

    Serial(int dimension, int posting_limit, int connection_limit, DistanceType distance_type, size_t element_size)
        : dimension_(dimension), posting_limit_(posting_limit), connection_limit_(connection_limit), node_num_(0),
            update_counter_(0), distance_type_(distance_type), element_size_(element_size), entry_point_(nullptr){}

    ~Serial() {

    }

    int getNodeNum() const { return node_num_; }
    int getUpdateCounter() { return update_counter_++;}
    int getPositingLimit() const { return posting_limit_; }
    int getDimension() const { return dimension_; }
    size_t getElementSize() const { return element_size_; }
    int getConnectionLimit() const { return connection_limit_; }

    Node *findExactNearestNode(const void *query_vector, Distance *distance);
    std::vector<Node*> findExactNearestNodes(const void *query_vector, Distance *distance, int n);
    std::vector<Node*> findNearestNodes(const void *query_vector, Distance *distance, int node_num, int pq_size);
    std::vector<int> topKSearch(const void *query_vector, int top_k, Distance *distance, int node_num, int pq_size);
    DistanceType getDistanceType() const { return distance_type_; }
    bool insertLock() { return insert_lock_.w_trylock(); }
    void insertUnlock() { return insert_lock_.w_unlock(); }
    void setEntryPoint(Node *node) { entry_point_ = node; }
    void incrementNodeNum() { node_num_++; }
    void addNode(Node *node) { all_nodes_.push_back(node); }
    void deleteNode(Node *node) {
        all_nodes_.erase(std::remove(all_nodes_.begin(), all_nodes_.end(), node), all_nodes_.end());
    }
    int countLostVectors(){
        int count = 0;
        std::unordered_set<int> indexed_ids;
        for (Node* node : all_nodes_) {
            Entity **posting = node->getPosting();
            for (int i = 0; i < node->getVectorNum(); ++i) {
                indexed_ids.insert(posting[i]->id);
                count++;
            }
        }
        return count - indexed_ids.size();
    }
    int countUniqueVectors(){
        std::unordered_set<int> indexed_ids;
        for (Node* node : all_nodes_) {
            Entity **posting = node->getPosting();
            for (int i = 0; i < node->getVectorNum(); ++i) {
                indexed_ids.insert(posting[i]->id);
            }
        }
        return indexed_ids.size();
    }
    int countAllVectors() {
        int count = 0;
        for (Node *node : all_nodes_) {
            count += node->getVectorNum();
        }
        return count;
    }

    bool verifyClusterAssignments(Distance *distance);
    int countViolateVectors(Distance *distance);

private:
    const int dimension_;
    const int posting_limit_;
    const int connection_limit_;
    int node_num_;
    int update_counter_;
    size_t element_size_;
    Lock insert_lock_;
    Node *entry_point_;
    DistanceType distance_type_;
    std::vector<Node*> all_nodes_;

};

class SerialInsertExecutor {
public:
    SerialInsertExecutor(Serial *serial, const void *insert_vector, int vector_id)
        : serial_(serial), insert_vector_(insert_vector), vector_id_(vector_id) {
        switch (serial_->getDistanceType()) {
            case Serial::L2:
                distance_ = new L2Distance();
                break;
            case Serial::Angular:
                distance_ = new AngularDistance();
                break;
        }
    }

    ~SerialInsertExecutor() {
        delete distance_;
    }

    void insert();

private:
    Serial *serial_;
    Distance *distance_;
    const void *insert_vector_;
    const int vector_id_;
    void split(Node *node, const void *insert_vector, int vector_id);
    void assignCalculation(Node *new_node1, Node *new_node2);
    void reassignCalculation(Node *spliting_node, Node *new_node1, Node *new_node2);
    void connectNeighbors(Node *spliting_node, Node *new_node1, Node *new_node2, int connection_limit);
    void updateNeighbors(Node *spliting_node, Node *new_node1, Node *new_node2, int connection_limit);
};

class SerialQueryExecutor {
public:
    SerialQueryExecutor(Serial *serial, const void *query_vector, int top_k, int node_num, int pq_size)
        : serial_(serial), query_vector_(query_vector), top_k_(top_k), node_num_(node_num), pq_size_(pq_size) {
        switch (serial_->getDistanceType()) {
            case Serial::L2:
                distance_ = new L2Distance();
                break;
            case Serial::Angular:
                distance_ = new AngularDistance();
                break;
        }
    }

    ~SerialQueryExecutor() {
        delete distance_;
    }

    std::vector<int> query(){
        return serial_->topKSearch(query_vector_, top_k_, distance_, node_num_, pq_size_);
    }

    private:
        Serial *serial_;
        const void *query_vector_;
        const int top_k_;
        const int node_num_;
        const int pq_size_;
        Distance *distance_;
};

#endif // SERIAL_H