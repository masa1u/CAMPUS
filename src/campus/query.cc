#include "campus.h"

std::vector<int> CampusQueryExecutor::query(){
    return campus_->topKSearch(query_vector_, top_k_, distance_, campus_->getNodeNum());
}