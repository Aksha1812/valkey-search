/*
 * Test program to demonstrate memory allocation debugging
 */

#include <iostream>
#include <memory>
#include <vector>

#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/memory_tracker.h"
#include "src/utils/string_interning.h"

int main() {
    std::cout << "=== Memory Allocation Debug Test ===" << std::endl;
    
    // Initialize Valkey allocator
    vmsdk::UseValkeyAlloc();
    
    // Create a memory pool for testing
    MemoryPool test_pool(0);
    
    std::cout << "\n1. Testing basic memory allocation..." << std::endl;
    {
        IsolatedMemoryScope scope(test_pool);
        
        // Allocate some memory using new/delete
        int* test_int = new int(42);
        std::cout << "Allocated int: " << *test_int << std::endl;
        delete test_int;
        
        // Allocate array
        char* test_array = new char[1024];
        std::cout << "Allocated 1024 byte array" << std::endl;
