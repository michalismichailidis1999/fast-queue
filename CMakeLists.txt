# CMakeList.txt : CMake project for FastQueue, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

# Enable Hot Reload for MSVC compilers if supported.
if (POLICY CMP0141)
  cmake_policy(SET CMP0141 NEW)
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT "$<IF:$<AND:$<C_COMPILER_ID:MSVC>,$<CXX_COMPILER_ID:MSVC>>,$<$<CONFIG:Debug,RelWithDebInfo>:EditAndContinue>,$<$<CONFIG:Debug,RelWithDebInfo>:ProgramDatabase>>")
endif()

project ("FastQueue")

set(SOURCE_DIR "src/source_files")
set(HEADER_DIR "src/header_files")

file(GLOB_RECURSE SOURCES "${SOURCE_DIR}/*.cpp")
file(GLOB_RECURSE HEADERS "${HEADER_DIR}/*.h")

# Add source to this project's executable.
add_executable (FastQueue "FastQueue.cpp" "FastQueue.h" ${SOURCES})

include_directories(${HEADER_DIR})

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET FastQueue PROPERTY CXX_STANDARD 20)
endif()

# TODO: Add tests and install targets if needed.
find_package(OpenSSL REQUIRED)
target_link_libraries(FastQueue PRIVATE OpenSSL::SSL)
target_link_libraries(FastQueue PRIVATE OpenSSL::Crypto)

find_package(lz4 CONFIG REQUIRED)
target_link_libraries(main PRIVATE lz4::lz4)