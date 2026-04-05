# CMakeList.txt : CMake project for FastQueue, include source and define
# project specific logic here.
#
cmake_minimum_required(VERSION 3.20)

# Enable Hot Reload for MSVC compilers if supported.
if (POLICY CMP0141)
  cmake_policy(SET CMP0141 NEW)
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT "$<IF:$<AND:$<C_COMPILER_ID:MSVC>,$<CXX_COMPILER_ID:MSVC>>,$<$<CONFIG:Debug,RelWithDebInfo>:EditAndContinue>,$<$<CONFIG:Debug,RelWithDebInfo>:ProgramDatabase>>")
endif()

project ("FastQueue")

set(SOURCE_DIR "./src/source_files")
set(HEADER_DIR "./src/header_files")

file(GLOB_RECURSE SOURCES "${SOURCE_DIR}/*.cpp")
file(GLOB_RECURSE HEADERS "${HEADER_DIR}/*.h")

# Add source to this project's executable.
add_executable (FastQueue "FastQueue.cpp" "FastQueue.h" ${SOURCES})

include_directories(${HEADER_DIR})

set_property(TARGET FastQueue PROPERTY CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

# TODO: Add tests and install targets if needed.
find_package(OpenSSL REQUIRED)
target_link_libraries(FastQueue PRIVATE OpenSSL::SSL)
target_link_libraries(FastQueue PRIVATE OpenSSL::Crypto)

find_package(Boost REQUIRED COMPONENTS asio)
target_link_libraries(FastQueue PRIVATE Boost::asio)

find_package(ZLIB REQUIRED)
target_link_libraries(FastQueue PRIVATE ZLIB::ZLIB)

# --- HWLOC Generic Search ---
find_path(HWLOC_INCLUDE_DIR NAMES hwloc.h)
find_library(HWLOC_LIBRARY NAMES hwloc)

if(HWLOC_INCLUDE_DIR AND HWLOC_LIBRARY)
    # Create a "fake" target that behaves like ZLIB::ZLIB
    if(NOT TARGET hwloc::hwloc)
        add_library(hwloc::hwloc UNKNOWN IMPORTED)
        set_target_properties(hwloc::hwloc PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${HWLOC_INCLUDE_DIR}"
            IMPORTED_LOCATION "${HWLOC_LIBRARY}"
        )
    endif()
else()
    message(FATAL_ERROR "hwloc not found! Ensure it is installed via vcpkg (Windows) or apt/brew (Linux/macOS).")
endif()

target_link_libraries(FastQueue PRIVATE hwloc::hwloc)