#pragma once

#if !defined(_WIN32) && !defined(_WIN64)  // Only for non-Windows

#include <cstring>  // memcpy, memset
#include <cerrno>   // EINVAL, ERANGE

typedef int errno_t; // define errno_t for Linux

inline errno_t memcpy_s(void* dest, size_t destsz, const void* src, size_t count) {
    if (!dest || !src) return EINVAL;

    if (count > destsz) {
        memset(dest, 0, destsz); // optional: clear destination
        return ERANGE;
    }

    memcpy(dest, src, count);

    return 0;
}

#endif