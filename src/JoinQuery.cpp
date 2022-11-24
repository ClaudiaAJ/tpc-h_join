#include "JoinQuery.hpp"
#include <assert.h>
#include <fstream>
#include <iostream>
#include <thread>
#include <unordered_set>
#include <algorithm>
#include "../thirdparty/parser.hpp"

using namespace aria::csv;

//---------------------------------------------------------------------------

JoinQuery::JoinQuery(const std::string& lineitem, const std::string& orders,
                     const std::string& customer)
{
    path_to_lineitem = lineitem;
    path_to_orders = orders;
    path_to_customer = customer;
};
//---------------------------------------------------------------------------
size_t JoinQuery::avg(const std::string& segmentParam)
{
    // start: add c_custkey to list if c_mktsegment matches segmentParam
    std::ifstream c(path_to_customer);
    CsvParser customer = CsvParser(c).delimiter('|');

    std::unordered_set<int> cust_ht;

    for (auto cust : customer) {
        if (cust[6] == segmentParam) {
            cust_ht.insert(stoi(cust[0]));
        }
    }
    assert(cust_ht.size() > 0);
    // end


    // start: add o_orderkey to list if o_custkey is in custkeys list
    std::ifstream o(path_to_orders);
    CsvParser orders = CsvParser(o).delimiter('|');

    std::unordered_set<int> cust_orders_ht;

    for (auto ord : orders) {
        if (cust_ht.find(stoi(ord[1])) != cust_ht.end()) {
            cust_orders_ht.insert(stoi(ord[0]));
        }
    }
    // end


    // start: compute avg(l_quantity) * 100
    double sum = 0;
    double count = 0;

    std::ifstream l(path_to_lineitem);
    CsvParser lineitem = CsvParser(l).delimiter('|');

    for (auto line : lineitem) {
        if (cust_orders_ht.find(stoi(line[0])) != cust_orders_ht.end()) {
            sum += stoi(line[4]);
            count += 1;
        }
    }
    // end
    return sum / count * 100;
}
//---------------------------------------------------------------------------
size_t JoinQuery::lineCount(std::string rel)
{
   std::ifstream relation(rel);
   assert(relation);  // make sure the provided string references a file
   size_t n = 0;
   for (std::string line; std::getline(relation, line);) n++;
   return n;
}
//---------------------------------------------------------------------------