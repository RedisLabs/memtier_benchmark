add_executable(hdr_decoder
    hdr_decoder.c)
target_link_libraries(hdr_decoder
    PRIVATE
        $<$<BOOL:${WIN32}>:hdr_histogram_static>
        $<$<NOT:$<BOOL:${WIN32}>>:hdr_histogram>)
install(
    TARGETS hdr_decoder
    EXPORT ${PROJECT_NAME}-targets
    DESTINATION ${CMAKE_INSTALL_BINDIR})

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
    find_package(Threads)

    add_executable(hiccup
        hiccup.c)
    target_link_libraries(hiccup
        PRIVATE
            hdr_histogram
            Threads::Threads)
    install(
        TARGETS hiccup
        EXPORT ${PROJECT_NAME}-targets
        DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()
