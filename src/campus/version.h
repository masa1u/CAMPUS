#ifndef CAMPUS_VERSION_H
#define CAMPUS_VERSION_H

#include "entity.h"
#include <vector>

class Version {
public:
    Version(int node_id, int version, Version *prev_version, int max_size, int dimension, size_t element_size)
        : node_id_(node_id), version_(version), prev_version_(prev_version), max_size_(max_size), size_(0),
            dimension_(dimension), element_size_(element_size) {posting_ = new Entity*[max_size_];}

    ~Version() {
        for (int i = 0; i < size_; ++i) {
            delete posting_[i];
        }
        delete[] posting_;
    }

    int getNodeID() const { return node_id_; }
    int getVersion() const { return version_; }
    int getSize() const { return size_; }
    const void* getCentroid() const { return centroid; }
    std::vector<int> getNeighbors() const { return neighbors_; }
    Entity **getPosting() const { return posting_; }
    void calculateCentroid();
    bool canAddVector() const { return size_ < max_size_; }
    void addVector(const void* vector, const int vector_id);
    void copyPostingFromPrevVersion();

private:
    const int node_id_;
    const int version_;
    Version *prev_version_;
    const int max_size_;
    int size_;
    int dimension_;
    size_t element_size_;
    std::vector<int> neighbors_;
    void *centroid;
    Entity **posting_;
};

#endif //CAMPUS_VERSION_H