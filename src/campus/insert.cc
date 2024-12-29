#include "campus.h"

// void CampusInsertExecutor::insert(){
//     // If the campus is empty, create a new node and set it as the entry point
//     if (campus_->getNodeSize() == 0) { 
//         Node *new_node = new Node(campus_->getNodeSize(), campus_->getPositionLimit(), campus_->getDimension(), sizeof(float));
//         while (!campus_->validationLock()) {}
//         campus_->addNodeToGraph(new_node);
//         Version *latest_version = new_node->getLatestVersion();
//         latest_version->addVector(insert_vector_, vector_id_);
//         latest_version->calculateCentroid();
//         campus_->validationUnlock();
//         return;
//     }else{
//         // Find the nearest node to the insert_vector_
//         Node *nearest_node = campus_->findExactNearestNode(insert_vector_, distance_);
//         Version *latest_version = nearest_node->getLatestVersion();
//         if (latest_version->canAddVector()) {
//             while (!campus_->validationLock()) {}
//             latest_version->addVector(insert_vector_, vector_id_);
//             latest_version->calculateCentroid();
//             campus_->validationUnlock();
//             return;
//         }else{
//             // faho
//         }
//     }
// }

void CampusInsertExecutor::insert(){
    // If the campus is empty, create a new node and set it as the entry point
    if (campus_->getNodeSize() == 0) { 
        Node *new_node = new Node(campus_->getNodeSize(), campus_->getPositingLimit(),
            campus_->getDimension(), sizeof(float));
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
        changed_versions_.push_back(latest_version);
        if (latest_version->canAddVector()) {
            // No need to split
            Version *new_version = new Version(nearest_node->getNodeId(), latest_version->getVersion() + 1,
                latest_version, campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());
            new_version->copyPostingFromPrevVersion();
            new_version->addVector(insert_vector_, vector_id_);
        }else{
            // Need to split
            splitCalculation(latest_version);
        }
    }

    if (validation()){
        return;
    }else{
        abort();
    }
}

void CampusInsertExecutor::splitCalculation(Version *spliting_version){
    std::vector<int> neighbors = spliting_version->getNeighbors();
    Node *new_node1 = new Node(campus_->getNodeSize(), campus_->getPositingLimit(),
        campus_->getDimension(), campus_->getElementSize());
    Node *new_node2 = new Node(campus_->getNodeSize() + 1, campus_->getPositingLimit(),
        campus_->getDimension(), campus_->getElementSize());
    new_nodes_.push_back(new_node1);
    new_nodes_.push_back(new_node2);
    Entity **posting = spliting_version->getPosting();

    // randomly assign vectors to new nodes
    for (int i = 0; i < spliting_version->getSize(); ++i) {
        const void *vector = posting[i]->getVector();
        int vector_id = posting[i]->id;
        if (i < spliting_version->getSize() / 2) {
            new_node1->getLatestVersion()->addVector(vector, vector_id);
        }else{
            new_node2->getLatestVersion()->addVector(vector, vector_id);
        }
    }

   assignCalculation(new_node1, new_node2);
   reassignCalculation(spliting_version, new_node1, new_node2);
}

void CampusInsertExecutor::assignCalculation(Node *new_node1, Node *new_node2) {
    // k-means clustering for the new nodes
    while (true) {
        new_node1->getLatestVersion()->calculateCentroid();
        new_node2->getLatestVersion()->calculateCentroid();
        bool changed = false;
        for (int i = 0; i < new_node1->getLatestVersion()->getSize(); ++i) {
            const void *vector = new_node1->getLatestVersion()->getPosting()[i]->getVector();
            int vector_id = new_node1->getLatestVersion()->getPosting()[i]->id;
            float distance1 = distance_->calculateDistance(vector,
                new_node1->getLatestVersion()->getCentroid(), campus_->getDimension());
            float distance2 = distance_->calculateDistance(vector,
                new_node2->getLatestVersion()->getCentroid(), campus_->getDimension());
            if (distance1 > distance2) {
                new_node1->getLatestVersion()->getPosting()[i]
                    = new_node2->getLatestVersion()->getPosting()[i];
                new_node2->getLatestVersion()->addVector(vector, vector_id);
                changed = true;
            }
        }
        for (int i = 0; i < new_node2->getLatestVersion()->getSize(); ++i) {
            const void *vector = new_node2->getLatestVersion()->getPosting()[i]->getVector();
            int vector_id = new_node2->getLatestVersion()->getPosting()[i]->id;
            float distance1 = distance_->calculateDistance(vector,
                new_node1->getLatestVersion()->getCentroid(), campus_->getDimension());
            float distance2 = distance_->calculateDistance(vector,
                new_node2->getLatestVersion()->getCentroid(), campus_->getDimension());
            if (distance1 < distance2) {
                new_node2->getLatestVersion()->getPosting()[i]
                    = new_node1->getLatestVersion()->getPosting()[i];
                new_node1->getLatestVersion()->addVector(vector, vector_id);
                changed = true;
            }
        }
        if (!changed) {
            break;
        }
    }
}

void CampusInsertExecutor::reassignCalculation(Version *spliting_version, Node *new_node1, Node *new_node2){
    std::vector<int> neighbors = spliting_version->getNeighbors();
    for (int neighbor_id : neighbors) {
        Node *neighbor = campus_->getNode(neighbor_id);
        if (neighbor == nullptr) {
            continue;
        }
        Entity **posting = neighbor->getLatestVersion()->getPosting();
        for (void *vector : posting->getVector()) {
            float distance1 = distance_->calculateDistance(vector,
                new_node1->getLatestVersion()->getCentroid(), campus_->getDimension());
            float distance2 = distance_->calculateDistance(vector,
                new_node2->getLatestVersion()->getCentroid(), campus_->getDimension());
            if (distance1 < distance2) {
                new_node1->addNeighbor(neighbor_id);
            }else{
                new_node2->addNeighbor(neighbor_id);
            }
        }
    }
    
}

bool CampusInsertExecutor::validation(){
    campus_->validationLock();
    for (Version * version : changed_versions_){
        if (version->getNeighbors().size() > 2){
            campus_->validationUnlock();
            return false;
        }
    }
    // 競合してなくてもnodeが追加されていたらnode_idを更新
    campus_->validationUnlock();
}

void CampusInsertExecutor::abort(){
    changed_versions_.clear();
    new_nodes_.clear();
    new_versions_.clear();
    insert();
}