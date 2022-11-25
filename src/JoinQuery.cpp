#include "JoinQuery.hpp"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <charconv>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <vector>

//---------------------------------------------------------------------------
JoinQuery::JoinQuery(const std::string& lineitem, const std::string& order,
                     const std::string& customer)
{
    path_to_lineitem = lineitem;
    path_to_orders = order;
    path_to_customer = customer;
}

//---------------------------------------------------------------------------
uint64_t fast_ToInt(const char* a, const char* b)
{
    int64_t result;
    for (result = 0; a != b; a++) {
        result = result * 10 + ((*a) - '0');
    }
    return result;
}

std::unordered_set<int> make_customer_ht(const std::string& path_to_customer,
                                        const std::string& segmentParam)
{
    std::unordered_set<int> customers;

    int file_desc_cust = open(path_to_customer.c_str(), O_RDONLY);
    if (file_desc_cust == -1) return customers;
    struct stat buffer_cust;
    if (fstat(file_desc_cust, &buffer_cust) == -1) return customers;
    size_t buffer_size_cust = buffer_cust.st_size;
    void* ptr_cust = mmap(nullptr, buffer_size_cust, PROT_READ, MAP_SHARED, file_desc_cust, 0);
    char* ptr_cust_current = static_cast<char*>(ptr_cust);
    char* ptr_cust_next;

    while ((ptr_cust_next = static_cast<char*>(memchr(ptr_cust_current, '\n', buffer_size_cust)))) {
        size_t line_len = ptr_cust_next - ptr_cust_current;

        char* column_ptr = ptr_cust_current;

        buffer_size_cust -= line_len + 1;
        ptr_cust_current = ptr_cust_next + 1;

        int customer_id;
        char* column_ptr_prev = column_ptr;
        for (size_t column_idx = 0; (column_ptr = static_cast<char*>(memchr(column_ptr, '|', line_len)));
        column_ptr++, column_idx++, line_len -= (column_ptr - column_ptr_prev), column_ptr_prev = column_ptr) {
            size_t len = column_ptr - column_ptr_prev;
            if (column_idx == 0) {
                customer_id = fast_ToInt(column_ptr_prev, column_ptr);
            } else if (column_idx == 6) {
                if (segmentParam.size() == len &&
                    memcmp(column_ptr_prev, segmentParam.c_str(), len) == 0) {
                    customers.insert(customer_id);
                }
                break;
            }
        }
    }
    munmap(ptr_cust, buffer_cust.st_size);
    close(file_desc_cust);
    return customers;
}

std::unordered_set<int> make_cust_orders_ht(const std::string& path_to_orders,
                                     const std::unordered_set<int>& customers)
{
    std::unordered_set<int> orders;

    int file_desc_ord = open(path_to_orders.c_str(), O_RDONLY);
    if (file_desc_ord == -1) return orders;
    struct stat buffer_ord;
    if (fstat(file_desc_ord, &buffer_ord) == -1) return orders;
    size_t buffer_size_ord = buffer_ord.st_size;
    void* ptr_ord = mmap(nullptr, buffer_size_ord, PROT_READ, MAP_SHARED, file_desc_ord, 0);
    char* ptr_ord_current = static_cast<char*>(ptr_ord);
    char* ptr_ord_next;

    while ((ptr_ord_next = static_cast<char*>(memchr(ptr_ord_current, '\n', buffer_size_ord)))) {
        size_t line_len = ptr_ord_next - ptr_ord_current;

        char* column_ptr = ptr_ord_current;

        buffer_size_ord -= line_len + 1;
        ptr_ord_current = ptr_ord_next + 1;

        int order_id;
        char* column_ptr_prev = column_ptr;
        for (size_t column_idx = 0; (column_ptr = static_cast<char*>(memchr(column_ptr, '|', line_len)));
        column_ptr++, column_idx++, line_len -= (column_ptr - column_ptr_prev), column_ptr_prev = column_ptr) {
            if (column_idx == 0) {
                order_id = fast_ToInt(column_ptr_prev, column_ptr);
            } else if (column_idx == 1) {
                int customer_id = fast_ToInt(column_ptr_prev, column_ptr);
                if (customers.find(customer_id) != customers.end()) {
                    orders.insert(order_id);
                }
                break;
            }
        }
    }
    munmap(ptr_ord, buffer_ord.st_size);
    close(file_desc_ord);
    return orders;
}

size_t JoinQuery::avg(const std::string& segmentParam)
{
    auto customers = make_customer_ht(path_to_customer, segmentParam);
    auto orders = make_cust_orders_ht(path_to_orders, customers);

    uint64_t count = 0;
    uint64_t sum = 0;

    int file_desc = open(path_to_lineitem.c_str(), O_RDONLY);
    if (file_desc == -1) return -1;
    struct stat buffer;
    if (fstat(file_desc, &buffer) == -1) return -1;
    size_t buffer_size = buffer.st_size;
    void* ptr = mmap(nullptr, buffer_size, PROT_READ, MAP_SHARED, file_desc, 0);
    char* ptr_current = static_cast<char*>(ptr);
    char* ptr_next;

    while ((ptr_next = static_cast<char*>(memchr(ptr_current, '\n', buffer_size)))) {
        size_t line_len = ptr_next - ptr_current;

        char* column_ptr = ptr_current;

        buffer_size -= line_len + 1;
        ptr_current = ptr_next + 1;

        char* column_ptr_prev = column_ptr;
        for (size_t column_idx = 0; (column_ptr = static_cast<char*>(memchr(column_ptr, '|', line_len)));
        column_ptr++, column_idx++, line_len -= (column_ptr - column_ptr_prev), column_ptr_prev = column_ptr) {
            if (column_idx == 0) {
                if (orders.find(fast_ToInt(column_ptr_prev, column_ptr)) ==
                    orders.end()) {
                    break;
                }
            } else if (column_idx == 4) {
                sum += fast_ToInt(column_ptr_prev, column_ptr);
                count++;
                break;
            }
        }
    }

    munmap(ptr, buffer.st_size);
    close(file_desc);

    assert(count != 0);
    return sum * 100 / count; // why does (sum / count * 100) not work??
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
