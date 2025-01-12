#include "nocontrol.h"

void NoControlInsertExecutor::insert(){
    if (nocontrol_->getNodeNum()==0){
        Node *new_node = new Node(10, 10, sizeof(float));
        nocontrol_->setEntryPoint(new_node);
        new_node->addVector(insert_vector_, vector_id_);
    }else{
        Node *nearest_node = nocontrol_->findExactNearestNode(insert_vector_);
        if (nearest_node->canAddVector()){
            nearest_node->addVector(insert_vector_, vector_id_);
        }else{
            split(nearest_node, insert_vector_, vector_id_);
        }    
    }
}

void NoControlInsertExecutor::split(Node *spliting_node, const void *insert_vector, const int vector_id){
    Node *new_node1 = new Node(10, 10, sizeof(float));
    Node *new_node2 = new Node(10, 10, sizeof(float));
    Entity **posting = spliting_node->getPosting();
    for (int i = 0; i < spliting_node->getVectorNum(); ++i) {
        const void *vector = posting[i]->getVector();
        if (i < spliting_node->getVectorNum() / 2) {
            new_node1->addVector(vector, posting[i]->id);
        }else{
            new_node2->addVector(vector, posting[i]->id);
        }
    }
    new_node1->addVector(insert_vector, vector_id);

    assign(new_node1, new_node2);
    connectNeighbors(spliting_node, new_node1, new_node2, nocontrol_->getConnectionLimit());
}

void NoControlInsertExecutor::assign(Node *new_node1, Node *new_node2){
    while (true) {
        new_node1->calculateCentroid();
        new_node2->calculateCentroid();
        bool changed = false;
        for (int i = 0; i < new_node1->getVectorNum(); ++i) {
            const void *vector = new_node1->getPosting()[i]->getVector();
            int vector_id = new_node1->getPosting()[i]->id;
            float distance1 = distance_->calculateDistance(vector, new_node1->getCentroid(), nocontrol_->getDimension());
            float distance2 = distance_->calculateDistance(vector, new_node2->getCentroid(), nocontrol_->getDimension());
            if (distance1 > distance2) {
                new_node1->deleteVector(vector_id);
                new_node2->addVector(vector, vector_id);
                changed = true;
                i--;
            }
        }
        for (int i = 0; i < new_node2->getVectorNum(); ++i) {
            const void *vector = new_node2->getPosting()[i]->getVector();
            int vector_id = new_node2->getPosting()[i]->id;
            float distance1 = distance_->calculateDistance(vector, new_node1->getCentroid(), nocontrol_->getDimension());
            float distance2 = distance_->calculateDistance(vector, new_node2->getCentroid(), nocontrol_->getDimension());
            if (distance1 < distance2) {
                new_node2->deleteVector(vector_id);
                new_node1->addVector(vector, vector_id);
                changed = true;
                i--;
            }
        }
        if (!changed) {
            break;
        }
    }
}

void NoControlInsertExecutor::connectNeighbors(Node *spliting_node, Node *new_node1, Node *new_node2, int connection_limit){
    std::vector<Node*> new_neighbors1 = spliting_node->getOutNeighbors();
    std::vector<Node*> new_neighbors2 = spliting_node->getOutNeighbors();

    new_neighbors1.push_back(new_node2);
    new_neighbors2.push_back(new_node1);

    if (new_neighbors1.size() > connection_limit){
        Node *farthest_neighbor = nullptr;
        float max_distance = 0;
        for (Node *neighbor : new_neighbors1){
            float distance = distance_->calculateDistance(new_node1->getCentroid(), neighbor->getCentroid(), nocontrol_->getDimension());
            if (distance > max_distance){
                max_distance = distance;
                farthest_neighbor = neighbor;
            }
        }
        new_neighbors1.erase(std::remove(new_neighbors1.begin(), new_neighbors1.end(), farthest_neighbor), new_neighbors1.end());
    }

    if (new_neighbors2.size() > connection_limit){
        Node *farthest_neighbor = nullptr;
        float max_distance = 0;
        for (Node *neighbor : new_neighbors2){
            float distance = distance_->calculateDistance(new_node2->getCentroid(), neighbor->getCentroid(), nocontrol_->getDimension());
            if (distance > max_distance){
                max_distance = distance;
                farthest_neighbor = neighbor;
            }
        }
    }

    for (Node *neighbor : new_neighbors1){
        neighbor->deleteInNeighbor(spliting_node);
        neighbor->addInNeighbor(new_node1);
        new_node1->addOutNeighbor(neighbor);
    }
    for (Node *neighbor : new_neighbors2){
        neighbor->deleteInNeighbor(spliting_node);
        neighbor->addInNeighbor(new_node2);
        new_node2->addOutNeighbor(neighbor);
    }
}

void NoControlInsertExecutor::updateNeighbors(Node *spliting_node, Node *new_node1, Node *new_node2, int connection_limit){
    std::vector<Node*> neighbors = spliting_node->getInNeighbors();
    for (Node *neighbor : neighbors){
        neighbor->deleteOutNeighbor(spliting_node);
        neighbor->addOutNeighbor(new_node1);
        new_node1->addInNeighbor(neighbor);
        neighbor->addOutNeighbor(new_node2);
        new_node2->addInNeighbor(neighbor);
    }
}
