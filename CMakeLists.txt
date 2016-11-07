cmake_minimum_required(VERSION 3.6)
project(assign3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
        buffer_mgr.h
        buffer_mgr_stat.c
        buffer_mgr_stat.h
        dberror.c
        dberror.h
        dt.h
        storage_mgr.h
        #test_assign2_1.c
        test_helper.h
        storage_mgr.c
        buffer_mgr.c
        expr.h
        record_mgr.h
        tables.h
        rm_serializer.c
        test_assign3_1.c
        test_expr.c
        expr.c
        record_mgr.c)
add_executable(assign3 ${SOURCE_FILES})