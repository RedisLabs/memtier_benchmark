cmake_minimum_required(VERSION 3.5)

project(hdr_histogram VERSION 0.10.0 LANGUAGES C)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Follow all steps below in order to calculate new ABI version when updating the library
# NOTE: THIS IS UNRELATED to the actual project version
#
# 1. If the library source code has changed at all since the last update, then increment revision
# 2. If any interfaces have been added, removed, or changed since the last update, increment current and set revision to 0.
# 3. If any interfaces have been added since the last public release, then increment age.
# 4. If any interfaces have been removed since the last public release, then set age to 0.

set(HDR_SOVERSION_CURRENT   6)
set(HDR_SOVERSION_AGE       1)
set(HDR_SOVERSION_REVISION  0)

set(HDR_VERSION ${HDR_SOVERSION_CURRENT}.${HDR_SOVERSION_AGE}.${HDR_SOVERSION_REVISION})
set(HDR_SOVERSION ${HDR_SOVERSION_CURRENT})

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(UNIX)
    add_compile_options(
        -Wall
        -Wno-unknown-pragmas
        -Wextra
        -Wshadow
        -Winit-self
        -Wpedantic
        $<$<COMPILE_LANGUAGE:C>:-Wmissing-prototypes>)
    add_definitions(
        -D_GNU_SOURCE)
endif()

add_subdirectory(src)

option(HDR_HISTOGRAM_BUILD_PROGRAMS "Build tests and examples" ON)
if(HDR_HISTOGRAM_BUILD_PROGRAMS)
    include(CTest)

    if(BUILD_TESTING)
        add_subdirectory(test)
    endif()

    add_subdirectory(examples)
endif()

write_basic_package_version_file(
    ${PROJECT_BINARY_DIR}/${PROJECT_NAME}-config-version.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion)
configure_file(
    config.cmake.in
    ${PROJECT_NAME}-config.cmake
    @ONLY)
install(
    EXPORT ${PROJECT_NAME}-targets
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
    NAMESPACE ${PROJECT_NAME}::)
install(
    FILES
        ${PROJECT_BINARY_DIR}/${PROJECT_NAME}-config.cmake
        ${PROJECT_BINARY_DIR}/${PROJECT_NAME}-config-version.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME})

set(CPACK_GENERATOR "TGZ")
include(CPack)
