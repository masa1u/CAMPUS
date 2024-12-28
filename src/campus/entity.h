#ifndef CAMPUS_ENTITY_H
#define CAMPUS_ENTITY_H

#include <cstring>

struct Entity {
    int id;
    void *vector;
    int dimension;

    Entity(int id, const void* vec, int dim, size_t element_size) : id(id), dimension(dim) {
        vector = new char[dim * element_size];
        std::memcpy(vector, vec, dim * element_size);
    }

    ~Entity() {
        delete[] static_cast<char*>(vector);
    }

    const void* getVector() const {
        return vector;
    }
};

#endif //CAMPUS_ENTITY_H