#include "campus.h"

void CampusInsertExecutor::insert(){
    // If the campus is empty, create a new node and set it as the entry point
    if (campus_->getNodeSize() == 0) { 
        Node *new_node = new Node(campus_->getNodeSize(), campus_->getPositionLimit(), campus_->getDimension(), sizeof(float));
        while (!campus_->validationLock()) {}
        campus_->addNodeToGraph(new_node);
        Version *latest_version = new_node->getLatestVersion();
        latest_version->addVector(insert_vector_, vector_id_);
        latest_version->calculateCentroid();
        campus_->validationUnlock();
        return;
    }else{
        // Find the nearest node to the insert_vector_
        Node *nearest_node = campus_->findExactNearestNode(insert_vector_, distance_);
        Version *latest_version = nearest_node->getLatestVersion();
        while (!campus_->validationLock()) {}
        latest_version->addVector(insert_vector_, vector_id_);
        latest_version->calculateCentroid();
        campus_->validationUnlock();
        return;
    }
}