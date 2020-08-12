option(HDR_HISTOGRAM_BUILD_BENCHMARK "Build benchmark" OFF)
if(HDR_HISTOGRAM_BUILD_BENCHMARK)
    if(UNIX)
        enable_language(CXX)

        include(ExternalProject)

        ExternalProject_Add(google_benchmark
            URL ${PROJECT_SOURCE_DIR}/lib/benchmark-1.5.0.zip
            URL_MD5 1af48846917b9730bee1848f6e38f563
            CMAKE_ARGS -DBENCHMARK_ENABLE_TESTING=OFF -DBENCHMARK_ENABLE_INSTALL=OFF
            SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src
            BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-build
            INSTALL_COMMAND "")

        ExternalProject_Get_Property(google_benchmark SOURCE_DIR)
        set(GOOGLE_BENCHMARK_SOURCE_DIR ${SOURCE_DIR})
        ExternalProject_Get_Property(google_benchmark BINARY_DIR)
        set(GOOGLE_BENCHMARK_BINARY_DIR ${BINARY_DIR})

        add_executable(hdr_histogram_benchmark
            hdr_histogram_benchmark.cpp)
        add_dependencies(hdr_histogram_benchmark
            google_benchmark)
        target_include_directories(hdr_histogram_benchmark
            PRIVATE
                ${GOOGLE_BENCHMARK_SOURCE_DIR}/include)
        target_link_directories(hdr_histogram_benchmark
            PRIVATE
                ${GOOGLE_BENCHMARK_BINARY_DIR}/src)
        target_link_libraries(hdr_histogram_benchmark
            PRIVATE
                benchmark
                hdr_histogram_static)
        install(
            TARGETS hdr_histogram_benchmark
            DESTINATION ${CMAKE_INSTALL_BINDIR})
    else()
        message(WARNING "google.benchmark - microbenchmarks disabled on WIN32 platforms")
    endif()
endif()

function(hdr_histogram_add_test_executable NAME)
    add_executable(${NAME}
        ${NAME}.c
        hdr_test_util.h
        minunit.c
        minunit.h)
    target_link_libraries(${NAME}
        PRIVATE
            hdr_histogram_static)
    install(
        TARGETS ${NAME}
        DESTINATION ${CMAKE_INSTALL_BINDIR})
endfunction()

function(hdr_histogram_add_test NAME)
    hdr_histogram_add_test_executable(${NAME})
    add_test(${NAME} ${NAME})
endfunction()

hdr_histogram_add_test(hdr_histogram_test)
hdr_histogram_add_test(hdr_histogram_atomic_test)
hdr_histogram_add_test(hdr_histogram_log_test)
hdr_histogram_add_test(hdr_atomic_test)
if(UNIX)
    hdr_histogram_add_test(hdr_histogram_atomic_concurrency_test)
endif()

hdr_histogram_add_test_executable(hdr_histogram_perf)

foreach(V 2.0.1.logV0 2.0.6.logV1 2.0.7S.logV2 2.0.7S.logV3)
    configure_file(jHiccup-${V}.hlog jHiccup-${V}.hlog COPYONLY)
endforeach()

foreach(V 2.0.1.logV0 2.0.6.logV1 2.0.7S.logV2 2.0.7S.logV3)
    configure_file(jHiccup-${V}.hlog jHiccup-${V}.hlog COPYONLY)
endforeach()

foreach(V test_tagged_invalid_histogram.txt test_tagged_invalid_tag_key.txt test_tagged_invalid_timestamp.txt test_tagged_missing_histogram.txt)
    configure_file(${V} ${V} COPYONLY)
endforeach()
