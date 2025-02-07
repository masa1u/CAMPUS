#ifndef CAMPUS_H
#define CAMPUS_H

#include "node.h"
#include "../utils/distance.h"
#include "../utils/lock.h"
#include <vector>
#include <mutex>
#include <memory>
#include <unordered_set>

class Campus {
public:
    enum DistanceType {
        L2,
        Angular
    };

    Campus(int dimension, int posting_limit, int connection_limit, DistanceType distance_type, size_t element_size)
        : dimension_(dimension), posting_limit_(posting_limit), connection_limit_(connection_limit), node_num_(0),
            update_counter_(0), distance_type_(distance_type), element_size_(element_size), entry_point_(nullptr){}

    ~Campus() {

    }

    int getNodeNum() const { return node_num_; }
    int getUpdateCounter() { return update_counter_++;}
    int getPositingLimit() const { return posting_limit_; }
    int getDimension() const { return dimension_; }
    size_t getElementSize() const { return element_size_; }
    int getConnectionLimit() const { return connection_limit_; }

    Node *findExactNearestNode(const void *query_vector, Distance *distance);
    std::vector<Node*> findExactNearestNodes(const void *query_vector, Distance *distance, int n); // for debug
    std::vector<Node*> findNearestNodes(const void *query_vector, Distance *distance, int node_num, int pq_size);
    std::vector<int> topKSearch(const void *query_vector, int top_k, Distance *distance, int node_num, int pq_size);
    DistanceType getDistanceType() const { return distance_type_; }
    bool validationLock() { return validation_lock_.w_trylock(); }
    void validationUnlock() { return validation_lock_.w_unlock(); }
    void switchVersion(Node *node, Version *new_version);
    void setEntryPoint(Node *node) { entry_point_ = node; }
    void incrementNodeNum() { node_num_++; }
    void incrementUpdateCounter() { update_counter_++; }  
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

    void deleteAllArchivedNodes() {
        all_nodes_->erase(std::remove_if(all_nodes_->begin(), all_nodes_->end(),
            [](Node *node) {
                return node->isArchived();
            }), all_nodes_->end());
    }

    int countLostVectors() {
        std::unordered_set<int> indexed_ids;
        int all_vector_num = 0;
        for (Node *node : *all_nodes_) {
            if (!node->isArchived()) {
                Entity **posting = node->getLatestVersion()->getPosting();
                for (int i = 0; i < node->getLatestVersion()->getVectorNum(); ++i) {
                    all_vector_num++;
                    indexed_ids.insert(posting[i]->id);
                }
            }
        }
        return all_vector_num - indexed_ids.size();
    }

    int countUniqueVectors() {
        std::unordered_set<int> indexed_ids;
        for (Node *node : *all_nodes_) {
            if (!node->isArchived()) {
                Entity **posting = node->getLatestVersion()->getPosting();
                for (int i = 0; i < node->getLatestVersion()->getVectorNum(); ++i) {
                    indexed_ids.insert(posting[i]->id);
                }
            }
        }
        return indexed_ids.size();
    }

    int countAllVectors() {
        int count = 0;
        for (Node *node : *all_nodes_) {
            if (!node->isArchived()) {
                count += node->getLatestVersion()->getVectorNum();
            }
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
    Lock validation_lock_;
    Node *entry_point_;
    std::mutex mutex_;
    std::shared_ptr<std::vector<Node*>> all_nodes_ = std::make_shared<std::vector<Node*>>();
    DistanceType distance_type_;

};

class CampusInsertExecutor {
public:
    CampusInsertExecutor(Campus *campus, const void *insert_vector, int vector_id)
        : campus_(campus), insert_vector_(insert_vector), vector_id_(vector_id) {
        switch (campus_->getDistanceType()) {
            case Campus::L2:
                distance_ = new L2Distance();
                break;
            case Campus::Angular:
                distance_ = new AngularDistance();
                break;
        }
    }

    ~CampusInsertExecutor() {
        delete distance_;
    }

    void insert();


private:
    Campus *campus_;
    Distance *distance_;
    const void *insert_vector_;
    const int vector_id_;
    std::vector<Version*> changed_versions_;
    std::vector<Node*> new_nodes_; // Newly created nodes with split
    std::vector<Version*> new_versions_; // Newly created versions without split
    void splitCalculation(Version *spliting_version, const void *insert_vector, int vector_id);
    void assignCalculation(Node *new_node1, Node *new_node2);
    void reassignCalculation(Version *spliting_version, Node *new_node1, Node *new_node2);
    void connectNeighbors(Version *spliting_version, Node *new_node1, Node *new_node2, int connection_limit);
    void updateNeighbors(Version *spliting_version, Node *new_node1, Node *new_node2, int connection_limit);
    bool validation();
    void commit();
    void abort();

    int countAfterAllVectors() {
        // for debug
        int count = 0;
        for (Version *version : new_versions_) {
            count += version->getVectorNum();
        }
        return count;
    }
    int countBeforeAllVectors() {
        // for debug
        int count = 0;
        for (Version *version : changed_versions_) {
            count += version->getVectorNum();
        }
        return count;
    }
};

class CampusQueryExecutor {
public:
    CampusQueryExecutor(Campus *campus, const void *query_vector, int top_k, int node_num, int pq_size)
        : campus_(campus), query_vector_(query_vector), top_k_(top_k) , node_num_(node_num), pq_size_(pq_size) {
        switch (campus_->getDistanceType()) {
            case Campus::L2:
                distance_ = new L2Distance();
                break;
            case Campus::Angular:
                distance_ = new AngularDistance();
                break;
        }
    }

    ~CampusQueryExecutor() {
        delete distance_;
    }

    std::vector<int> query() {
        return campus_->topKSearch(query_vector_, top_k_, distance_, node_num_, pq_size_);
    };

    private:
        Campus *campus_;
        const void *query_vector_;
        const int top_k_;
        Distance *distance_;
        const int node_num_;
        const int pq_size_;
};

#endif // CAMPUS_H