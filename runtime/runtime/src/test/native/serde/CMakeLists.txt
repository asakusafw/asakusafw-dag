cmake_minimum_required(VERSION 2.8)

project(test-serde)

file(GLOB NATIVE "*.cpp")

include_directories("../../../main/resources/com/asakusafw/dag/runtime/io/native/include")

add_library(test-serde SHARED ${NATIVE})
set_target_properties(test-serde PROPERTIES COMPILE_FLAGS "-std=c++11 -Wall")
