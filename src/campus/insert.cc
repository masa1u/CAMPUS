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
        return;
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
   connectNeighbors(spliting_version, new_node1, new_node2, campus_->getConnectionLimit());
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

void CampusInsertExecutor::connectNeighbors(Version *spliting_version, Node *new_node1, Node *new_node2, int connection_limit){
    std::vector<int> neighbors1 = spliting_version->getNeighbors();
    std::vector<int> neighbors2 = spliting_version->getNeighbors();
    neighbors1.push_back(new_node2->getNodeId());
    neighbors2.push_back(new_node1->getNodeId());

    // if the number of neighbors exceeds the connection limit, remove the farthest neighbor
    if (neighbors1.size() > connection_limit) {
        float max_distance = 0;
        int farthest_neighbor = -1;
        for (int neighbor_id : neighbors1) {
            Node *neighbor = campus_->getNode(neighbor_id);
            float distance = distance_->calculateDistance(new_node1->getLatestVersion()->getCentroid(),
                neighbor->getLatestVersion()->getCentroid(), campus_->getDimension());
            if (distance > max_distance) {
                max_distance = distance;
                farthest_neighbor = neighbor_id;
            }
        }
        neighbors1.erase(std::remove(neighbors1.begin(), neighbors1.end(), farthest_neighbor), neighbors1.end());
    }
    if (neighbors2.size() > connection_limit) {
        float max_distance = 0;
        int farthest_neighbor = -1;
        for (int neighbor_id : neighbors2) {
            Node *neighbor = campus_->getNode(neighbor_id);
            float distance = distance_->calculateDistance(new_node2->getLatestVersion()->getCentroid(),
                neighbor->getLatestVersion()->getCentroid(), campus_->getDimension());
            if (distance > max_distance) {
                max_distance = distance;
                farthest_neighbor = neighbor_id;
            }
        }
        neighbors2.erase(std::remove(neighbors2.begin(), neighbors2.end(), farthest_neighbor), neighbors2.end());
    }

    for (int neighbor_id : neighbors1) {
        Node *neighbor = campus_->getNode(neighbor_id);
        if (neighbor == nullptr) {
            continue;
        }
        neighbor->getLatestVersion()->addNeighbor(new_node1->getNodeId());
        new_node1->getLatestVersion()->addNeighbor(neighbor_id);
    }
    for (int neighbor_id : neighbors2) {
        Node *neighbor = campus_->getNode(neighbor_id);
        if (neighbor == nullptr) {
            continue;
        }
        neighbor->getLatestVersion()->addNeighbor(new_node2->getNodeId());
        new_node2->getLatestVersion()->addNeighbor(neighbor_id);
    }
}

void CampusInsertExecutor::updateNeighbors(Version *spliting_version, Node *new_node1, Node *new_node2, int connection_limit) {
    std::vector<int> updating_neighbors = spliting_version->getNeighbors();
    for (int neighbor_id : updating_neighbors) {
        // 既に更新があって、new_versions_に含まれている場合はそのVersionを取得、含まれていない場合は新しいVersionを作成
        bool already_updated = false;
        Version *new_version = nullptr;
        for (Version *existing_version : new_versions_) {
            if (existing_version->getNodeID() == neighbor_id) {
                new_version = existing_version;
                already_updated = true;
                break;
            }
        }
        if (!already_updated) {
            Node *neighbor_node = campus_->getNode(neighbor_id);
            if (neighbor_node != nullptr) {
                new_version = new Version(neighbor_id, neighbor_node->getLatestVersion()->getVersion() + 1,
                    neighbor_node->getLatestVersion(), campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());
                new_versions_.push_back(new_version);
                new_version->copyNeighborFromPrevVersion();
                new_version->copyPostingFromPrevVersion();
            }
        }

        new_version->addNeighbor(new_node1->getNodeId());
        new_version->addNeighbor(new_node2->getNodeId());
        if (new_version->getNeighbors().size() > connection_limit) {
            float max_distance = 0;
            int farthest_neighbor = -1;
            for (int neighbor_id : new_version->getNeighbors()) {
                Node *neighbor = campus_->getNode(neighbor_id);
                float distance = distance_->calculateDistance(new_version->getCentroid(),
                    neighbor->getLatestVersion()->getCentroid(), campus_->getDimension());
                if (distance > max_distance) {
                    max_distance = distance;
                    farthest_neighbor = neighbor_id;
                }
            }
            new_version->getNeighbors().erase(std::remove(new_version->getNeighbors().begin(),
                new_version->getNeighbors().end(), farthest_neighbor), new_version->getNeighbors().end());
        

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
        for (int i = 0; i < neighbor->getLatestVersion()->getSize(); ++i) {
            const void *vector = posting[i]->getVector();
            const void *old_cetroid = neighbor->getLatestVersion()->getCentroid();
            const void *new_centoid1 = new_node1->getLatestVersion()->getCentroid();
            const void *new_centoid2 = new_node2->getLatestVersion()->getCentroid();
            float old_distance = distance_->calculateDistance(vector, old_cetroid, campus_->getDimension());
            float new_distance1 = distance_->calculateDistance(vector, new_centoid1, campus_->getDimension());
            float new_distance2 = distance_->calculateDistance(vector, new_centoid2, campus_->getDimension());
            if (old_distance < new_distance1 && old_distance < new_distance2) {
                continue;
            }else{
                if (new_distance1 < new_distance2) {
                    if (new_node1->getLatestVersion()->canAddVector()) {
                        new_node1->getLatestVersion()->addVector(vector, posting[i]->id);
                    }else{
                        // splitしたものがsplitされる場合
                        splitCalculation(new_node1->getLatestVersion());
                    }
                }else{
                    if (new_node2->getLatestVersion()->canAddVector()) {
                        new_node2->getLatestVersion()->addVector(vector, posting[i]->id);
                    }else{
                        splitCalculation(new_node2->getLatestVersion());
                    }
                }
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