#ifndef DISTANCE_H
#define DISTANCE_H

#include <cstddef>

class Distance {
public:
    Distance();
    virtual ~Distance() {}
    virtual float calculateDistance(const void *vector1, const void *vector2, size_t dimension) = 0;
};

class L2Distance : public Distance {
public:
    L2Distance();
    float calculateDistance(const void *vector1, const void *vector2, size_t dimension);
};

class AngularDistance : public Distance {
public:
    AngularDistance();
    float calculateDistance(const void *vector1, const void *vector2, size_t dimension);
};

#endif //DISTANCE_H