#ifndef SERIAL_NODE_H
#define SERIAL_NODE_H

#include "entity.h"
#include <vector>
#include <cassert>
#include <algorithm>

class Node {
public:
    Node(int max_num, int dimension, size_t element_size)
        : max_num_(max_num), vector_num_(0), dimension_(dimension), element_size_(element_size) {
        posting_ = new Entity*[max_num_];
        centroid = new char[dimension_ * element_size_];
    }

    ~Node() {
        for (int i = 0; i < vector_num_; ++i) {
            delete posting_[i];
        }
        delete[] posting_;
        delete[] static_cast<char*>(centroid);
    }

    int getVectorNum() const { return vector_num_; }
    void* getCentroid() const { return centroid; }
    std::vector<Node*> getInNeighbors() const { return in_neighbors_; }
    std::vector<Node*> getOutNeighbors() const { return out_neighbors_; }
    Entity **getPosting() const { return posting_; }
    void calculateCentroid();
    bool canAddVector() const { return vector_num_ < max_num_; }
    void addVector(const void* vector, const int vector_id);
    void deleteVector(int vector_id);

    void addInNeighbor(Node* neighbor);
    void deleteInNeighbor(Node* neighbor) {
        in_neighbors_.erase(std::remove(in_neighbors_.begin(), in_neighbors_.end(), neighbor), in_neighbors_.end());
    }
    void addOutNeighbor(Node* neighbor);
    void deleteOutNeighbor(Node* neighbor) {
        out_neighbors_.erase(std::remove(out_neighbors_.begin(), out_neighbors_.end(), neighbor), out_neighbors_.end());
    }

private:
    const int max_num_;
    int vector_num_;
    const int dimension_;
    const size_t element_size_;
    std::vector<Node*> in_neighbors_;
    std::vector<Node*> out_neighbors_;
    void *centroid;
    Entity **posting_;
};

#endif //CAMPUS_NODE_H
