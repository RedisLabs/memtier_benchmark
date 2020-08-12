find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)

include(CheckLibraryExists)
check_library_exists(m ceil "" HAVE_LIBM)
check_library_exists(rt clock_gettime "" HAVE_LIBRT)

set(HDR_HISTOGRAM_SOURCES
    hdr_encoding.c
    hdr_histogram.c
    hdr_histogram_log.c
    hdr_interval_recorder.c
    hdr_thread.c
    hdr_time.c
    hdr_writer_reader_phaser.c)

set(HDR_HISTOGRAM_PRIVATE_HEADERS
    hdr_atomic.h
    hdr_encoding.h
    hdr_endian.h
    hdr_tests.h)

set(HDR_HISTOGRAM_PUBLIC_HEADERS
    hdr_histogram.h
    hdr_histogram_log.h
    hdr_interval_recorder.h
    hdr_thread.h
    hdr_time.h
    hdr_writer_reader_phaser.h)

function(hdr_histogram_add_library NAME LIBRARY_TYPE DO_INSTALL)
    add_library(${NAME} ${LIBRARY_TYPE}
        ${HDR_HISTOGRAM_SOURCES}
        ${HDR_HISTOGRAM_PRIVATE_HEADERS}
        ${HDR_HISTOGRAM_PUBLIC_HEADERS})
    target_link_libraries(${NAME}
        PRIVATE
            ZLIB::ZLIB
            Threads::Threads
            $<$<BOOL:${HAVE_LIBM}>:m>
            $<$<BOOL:${HAVE_LIBRT}>:rt>
            $<$<BOOL:${WIN32}>:ws2_32>)
    target_include_directories(${NAME}
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    if(DO_INSTALL)
        install(
            TARGETS ${NAME}
            EXPORT ${PROJECT_NAME}-targets
            DESTINATION ${CMAKE_INSTALL_LIBDIR})
    endif()
endfunction()

option(HDR_HISTOGRAM_BUILD_SHARED "Build shared library" ON)
option(HDR_HISTOGRAM_INSTALL_SHARED "Install shared library" ON)
if(HDR_HISTOGRAM_BUILD_SHARED)
    hdr_histogram_add_library(hdr_histogram SHARED ${HDR_HISTOGRAM_INSTALL_SHARED})
    set_target_properties(hdr_histogram PROPERTIES
        VERSION ${HDR_VERSION}
        SOVERSION ${HDR_SOVERSION})
endif()

option(HDR_HISTOGRAM_BUILD_STATIC "Build static library" ON)
option(HDR_HISTOGRAM_INSTALL_STATIC "Install static library" ON)
if(HDR_HISTOGRAM_BUILD_STATIC)
    hdr_histogram_add_library(hdr_histogram_static STATIC ${HDR_HISTOGRAM_INSTALL_STATIC})
endif()

install(
    FILES ${HDR_HISTOGRAM_PUBLIC_HEADERS}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/hdr)
