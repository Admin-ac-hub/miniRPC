find_path(Liburing_INCLUDE_DIR
    NAMES liburing.h
)

find_library(Liburing_LIBRARY
    NAMES uring
)

set(_Liburing_VERSION_HEADER "${Liburing_INCLUDE_DIR}/liburing/io_uring_version.h")
if(EXISTS "${_Liburing_VERSION_HEADER}")
    file(STRINGS "${_Liburing_VERSION_HEADER}" _Liburing_VERSION_MAJOR_LINE
        REGEX "^#define[ \t]+IO_URING_VERSION_MAJOR[ \t]+[0-9]+"
    )
    file(STRINGS "${_Liburing_VERSION_HEADER}" _Liburing_VERSION_MINOR_LINE
        REGEX "^#define[ \t]+IO_URING_VERSION_MINOR[ \t]+[0-9]+"
    )
    string(REGEX REPLACE ".*[ \t]([0-9]+)$" "\\1"
        Liburing_VERSION_MAJOR "${_Liburing_VERSION_MAJOR_LINE}"
    )
    string(REGEX REPLACE ".*[ \t]([0-9]+)$" "\\1"
        Liburing_VERSION_MINOR "${_Liburing_VERSION_MINOR_LINE}"
    )
    if(Liburing_VERSION_MAJOR MATCHES "^[0-9]+$" AND
       Liburing_VERSION_MINOR MATCHES "^[0-9]+$")
        set(Liburing_VERSION "${Liburing_VERSION_MAJOR}.${Liburing_VERSION_MINOR}")
    endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Liburing
    REQUIRED_VARS
        Liburing_INCLUDE_DIR
        Liburing_LIBRARY
    VERSION_VAR Liburing_VERSION
)

if(Liburing_FOUND AND NOT TARGET Liburing::Liburing)
    add_library(Liburing::Liburing UNKNOWN IMPORTED)
    set_target_properties(Liburing::Liburing PROPERTIES
        IMPORTED_LOCATION "${Liburing_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${Liburing_INCLUDE_DIR}"
    )
endif()

mark_as_advanced(
    Liburing_INCLUDE_DIR
    Liburing_LIBRARY
)
