cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

project(native_filter 
  VERSION 0.1.0
  DESCRIPTION "A Fletcher native filter"
  HOMEPAGE_URL "https://github.com/teratide/dremio-accelerated"
  LANGUAGES CXX
)

find_package(Arrow 1.0.1 CONFIG REQUIRED)
find_package(JNI REQUIRED)
add_library(jni INTERFACE IMPORTED)
target_include_directories(jni INTERFACE "${JNI_INCLUDE_DIRS}")

include(FetchContent)

FetchContent_Declare(cmake-modules
  GIT_REPOSITORY  https://github.com/abs-tudelft/cmake-modules.git
  GIT_TAG         master
)
FetchContent_MakeAvailable(cmake-modules)

include(CompileUnits)

add_compile_unit(
  NAME jni_helpers
  PRPS
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/jni/Assertions.cpp
    src/jni/Converters.cpp
    src/jni/JavaResizableBuffer.cpp
  DEPS
    jni
    arrow_shared
)

add_compile_unit(
  NAME native_filter
  TYPE SHARED
  COMPONENT library
  PRPS
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/NativeFilter.cpp
  DEPS
    jni_helpers
    arrow_shared
)

compile_units()

execute_process (
    COMMAND bash -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
    OUTPUT_VARIABLE OS_NAME
)

execute_process (
    COMMAND bash -c "awk -F= '/^VERSION_ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
    OUTPUT_VARIABLE OS_VERSION
)

if(OS_NAME MATCHES "ubuntu")
  set(CPACK_DEBIAN_PACKAGE_RELEASE "ubuntu${OS_VERSION}")
  set(CPACK_GENERATOR "DEB")
elseif(OS_NAME MATCHES "centos")
  set(CPACK_RPM_PACKAGE_RELEASE_DIST "el${OS_VERSION}")
  if(OS_VERSION MATCHES "7")
    set(CPACK_GENERATOR "RPM;TGZ")
  else()
    set(CPACK_GENERATOR "RPM")
  endif()
endif()

set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
set(CPACK_COMPONENTS_ALL library)
set(CPACK_PACKAGE_VENDOR "Teratide")
set(CPACK_PACKAGE_DESCRIPTION "A Fletcher native filter")
set(CPACK_PACKAGE_VERSION_MAJOR "${native_filter_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${native_filter_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${native_filter_VERSION_PATCH}")
set(CPACK_PACKAGE_RELOCATABLE ON)

set(CPACK_DEBIAN_FILE_NAME "DEB-DEFAULT")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "openjdk-8-jdk-headless, libarrow-dev (>= 1.0.1), libarrow-dev (<< 2.0.0)")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "${CPACK_PACKAGE_VENDOR}")

set(CPACK_RPM_FILE_NAME "RPM-DEFAULT")
set(CPACK_RPM_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION}")
set(CPACK_RPM_PACKAGE_LICENSE "ASL 2.0")
set(CPACK_RPM_PACKAGE_REQUIRES "java-1.8.0-openjdk-devel, arrow-devel >= 1.0.1, arrow-devel < 2.0.0")

set(CPACK_ARCHIVE_LIBRARY_FILE_NAME "${CMAKE_PROJECT_NAME}-${native_filter_VERSION}-${CMAKE_SYSTEM_NAME}")

include(CPack)
