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

message(STATUS "CMake prefix path: ${CMAKE_PREFIX_PATH}")
message(STATUS "System: ${CMAKE_SYSTEM_NAME}")

set(SOURCE_DIR "./src/source_files")
set(HEADER_DIR "./src/header_files")

file(GLOB_RECURSE SOURCES "${SOURCE_DIR}/*.cpp")
file(GLOB_RECURSE HEADERS "${HEADER_DIR}/*.h")

# Add source to this project's executable.
add_executable (FastQueue "FastQueue.cpp" "FastQueue.h" ${SOURCES})

include_directories(${HEADER_DIR})

set_property(TARGET FastQueue PROPERTY CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

# ── IntelliSense include paths for WSL ────────────────────────────────
if(UNIX AND NOT APPLE)
    # these paths tell VS IntelliSense where headers live inside WSL
    target_include_directories(FastQueue PRIVATE
        /usr/include                        # standard system headers
        /usr/include/boost                  # boost headers
        /usr/include/x86_64-linux-gnu       # arch specific headers
    )
endif()

# TODO: Add tests and install targets if needed.
find_package(OpenSSL REQUIRED)
target_link_libraries(FastQueue PRIVATE OpenSSL::SSL)
target_link_libraries(FastQueue PRIVATE OpenSSL::Crypto)

# ── Boost ──────────────────────────────────────────────────────────────
if(WIN32)
    # Windows — vcpkg provides Boost::asio as a proper target
    find_package(Boost REQUIRED COMPONENTS system)
    target_link_libraries(FastQueue PRIVATE Boost::system)
else()
    # Linux — asio is header-only, link Boost::system for async support
    find_package(Boost REQUIRED COMPONENTS system)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(FastQueue PRIVATE Boost::system)
endif()

find_package(ZLIB REQUIRED)
target_link_libraries(FastQueue PRIVATE ZLIB::ZLIB)

# ── HWLOC ──────────────────────────────────────────────────────────────
if(WIN32)
    find_package(hwloc CONFIG QUIET)
    if(hwloc_FOUND)
        target_link_libraries(FastQueue PRIVATE hwloc::hwloc)
    else()
        find_path(HWLOC_INCLUDE_DIR NAMES hwloc.h)
        find_library(HWLOC_LIBRARY NAMES hwloc hwloc-15 libhwloc)
        if(HWLOC_INCLUDE_DIR AND HWLOC_LIBRARY)
            if(NOT TARGET hwloc::hwloc)
                add_library(hwloc::hwloc UNKNOWN IMPORTED)
                set_target_properties(hwloc::hwloc PROPERTIES
                    INTERFACE_INCLUDE_DIRECTORIES "${HWLOC_INCLUDE_DIR}"
                    IMPORTED_LOCATION "${HWLOC_LIBRARY}"
                )
            endif()
            target_link_libraries(FastQueue PRIVATE hwloc::hwloc)
        else()
            message(FATAL_ERROR "hwloc not found on Windows — install via: vcpkg install hwloc")
        endif()
    endif()
else()
    # Try pkg-config first (most reliable on Linux)
    message(STATUS "=== HWLOC DETECTION START ===")

    # Try pkg-config first (most reliable on Linux)
    find_package(PkgConfig QUIET)
    message(STATUS "PkgConfig found: ${PKGCONFIG_FOUND}")

    if(PKGCONFIG_FOUND)
        pkg_check_modules(HWLOC QUIET hwloc)
        message(STATUS "HWLOC via pkg-config found: ${HWLOC_FOUND}")
        message(STATUS "HWLOC libs: ${HWLOC_LINK_LIBRARIES}")
        message(STATUS "HWLOC includes: ${HWLOC_INCLUDE_DIRS}")
    endif()

    if(HWLOC_FOUND)
        message(STATUS "hwloc found via pkg-config: ${HWLOC_LINK_LIBRARIES}")

        # pkg-config omits /usr/include as it's a default system path
        if(NOT HWLOC_INCLUDE_DIRS)
            message(STATUS "HWLOC_INCLUDE_DIRS empty, falling back to /usr/include")
            set(HWLOC_INCLUDE_DIRS "/usr/include")
        endif()

        target_include_directories(FastQueue PRIVATE ${HWLOC_INCLUDE_DIRS})
        target_link_libraries(FastQueue PRIVATE ${HWLOC_LINK_LIBRARIES})
        target_compile_options(FastQueue PRIVATE "-I/usr/include")
    else()
        # Fallback: manual search with explicit hints
        message(STATUS "pkg-config failed, trying manual search...")
        
        find_path(HWLOC_INCLUDE_DIR
            NAMES hwloc.h
            HINTS /usr/include /usr/local/include
        )
        find_library(HWLOC_LIBRARY
            NAMES hwloc
            HINTS /usr/lib /usr/local/lib
                  /usr/lib/x86_64-linux-gnu
                  /usr/lib/aarch64-linux-gnu
        )
        if(HWLOC_INCLUDE_DIR AND HWLOC_LIBRARY)
            message(STATUS "hwloc found manually: ${HWLOC_LIBRARY}")
            if(NOT TARGET hwloc::hwloc)
                add_library(hwloc::hwloc UNKNOWN IMPORTED)
                set_target_properties(hwloc::hwloc PROPERTIES
                    INTERFACE_INCLUDE_DIRECTORIES "${HWLOC_INCLUDE_DIR}"
                    IMPORTED_LOCATION "${HWLOC_LIBRARY}"
                )
            endif()
            target_include_directories(FastQueue PRIVATE ${HWLOC_INCLUDE_DIR})
            target_link_libraries(FastQueue PRIVATE hwloc::hwloc)
        else()
            message(FATAL_ERROR
                "hwloc not found.\n"
                "  Install with: sudo apt install libhwloc-dev\n"
                "  Then verify:  pkg-config --libs hwloc"
            )
        endif()
    endif()

    message(STATUS "=== HWLOC DETECTION END ===")
endif()

# ── Platform specific libraries ────────────────────────────────────────
if(WIN32)
    target_link_libraries(FastQueue PRIVATE ws2_32 mswsock kernel32)
endif()

if(UNIX AND NOT APPLE)
    target_link_libraries(FastQueue PRIVATE pthread rt)
endif()