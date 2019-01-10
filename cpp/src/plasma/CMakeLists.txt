# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

add_custom_target(plasma-all)
add_custom_target(plasma)
add_custom_target(plasma-benchmarks)
add_custom_target(plasma-tests)
add_dependencies(plasma-all plasma plasma-tests plasma-benchmarks)

# For the moment, Plasma is versioned like Arrow
project(plasma VERSION "${ARROW_BASE_VERSION}")
set(PLASMA_VERSION "${ARROW_VERSION}")

find_package(Threads)

# The SO version is also the ABI version
set(PLASMA_SO_VERSION "${ARROW_SO_VERSION}")
set(PLASMA_FULL_SO_VERSION "${ARROW_FULL_SO_VERSION}")

include_directories("${FLATBUFFERS_INCLUDE_DIR}" "${CMAKE_CURRENT_LIST_DIR}/" "${CMAKE_CURRENT_LIST_DIR}/thirdparty/" "${CMAKE_CURRENT_LIST_DIR}/../")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-conversion")

# Compile flatbuffers

set(PLASMA_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/plasma.fbs" "${CMAKE_CURRENT_LIST_DIR}/format/common.fbs")
set(OUTPUT_DIR ${ARROW_BINARY_DIR}/src/plasma)

set(PLASMA_FBS_OUTPUT_FILES
  "${OUTPUT_DIR}/common_generated.h"
  "${OUTPUT_DIR}/plasma_generated.h")

add_custom_target(gen_plasma_fbs DEPENDS ${PLASMA_FBS_OUTPUT_FILES})

if(FLATBUFFERS_VENDORED)
  add_dependencies(gen_plasma_fbs flatbuffers_ep)
endif()

add_custom_command(
  OUTPUT ${PLASMA_FBS_OUTPUT_FILES}
  # The --gen-object-api flag generates a C++ class MessageT for each
  # flatbuffers message Message, which can be used to store deserialized
  # messages in data structures. This is currently used for ObjectInfo for
  # example.
  COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${PLASMA_FBS_SRC} --gen-object-api --scoped-enums
  DEPENDS ${PLASMA_FBS_SRC}
  COMMENT "Running flatc compiler on ${PLASMA_FBS_SRC}"
  VERBATIM)

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

set(PLASMA_SRCS
  client.cc
  common.cc
  eviction_policy.cc
  events.cc
  fling.cc
  io.cc
  malloc.cc
  plasma.cc
  protocol.cc
  thirdparty/ae/ae.c)

set(PLASMA_LINK_LIBS arrow_shared)
set(PLASMA_STATIC_LINK_LIBS arrow_static)

if (ARROW_CUDA)
  set(PLASMA_LINK_LIBS ${PLASMA_LINK_LIBS} arrow_cuda_shared)
  set(PLASMA_STATIC_LINK_LIBS arrow_cuda_static ${PLASMA_STATIC_LINK_LIBS})
  add_definitions(-DPLASMA_CUDA)
endif()

ADD_ARROW_LIB(plasma
  SOURCES ${PLASMA_SRCS}
  OUTPUTS PLASMA_LIBRARIES
  DEPENDENCIES gen_plasma_fbs
  SHARED_LINK_LIBS ${FLATBUFFERS_STATIC_LIB} ${CMAKE_THREAD_LIBS_INIT} ${PLASMA_LINK_LIBS}
  STATIC_LINK_LIBS ${FLATBUFFERS_STATIC_LIB} ${CMAKE_THREAD_LIBS_INIT} ${PLASMA_STATIC_LINK_LIBS})

add_dependencies(plasma ${PLASMA_LIBRARIES})

foreach(LIB_TARGET ${PLASMA_LIBRARIES})
  target_compile_definitions(${LIB_TARGET}
    PRIVATE ARROW_EXPORTING)
endforeach()

# The optimization flag -O3 is suggested by dlmalloc.c, which is #included in
# malloc.cc; we set it here regardless of whether we do a debug or release build.
set_source_files_properties(malloc.cc PROPERTIES
  COMPILE_FLAGS "-O3")

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  set_property(SOURCE malloc.cc
    APPEND_STRING
    PROPERTY COMPILE_FLAGS
    " -Wno-parentheses-equality \
-Wno-null-pointer-arithmetic \
-Wno-shorten-64-to-32 \
-Wno-unused-macros")
endif()

if ("${COMPILER_FAMILY}" STREQUAL "gcc")
  set_property(SOURCE malloc.cc
    APPEND_STRING
    PROPERTY COMPILE_FLAGS
    " -Wno-conversion")
endif()

list(APPEND PLASMA_EXTERNAL_STORE_SOURCES "external_store_worker.cc" "external_store.cc" "hash_table_store.cc")
if (ARROW_PLASMA_S3)
  set(ZLIB_VERSION "1.2.11")
  set(OPENSSL_VERSION "1.1.1-pre7")
  set(CURL_VERSION "7.60.0")
  set(AWSSDK_VERSION "1.4.26")

  set(ZLIB_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(ZLIB_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(ZLIB_PREFIX "${PROJECT_BINARY_DIR}/external/zlib")
  set(ZLIB_INCLUDE_DIR "${ZLIB_PREFIX}/include")
  set(ZLIB_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_CXX_FLAGS=${ZLIB_CXX_FLAGS}"
          "-DCMAKE_C_FLAGS=${ZLIB_C_FLAGS}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}"
          "-DBUILD_SHARED_LIBS=OFF")

  set(ZLIB_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}z")
  set(ZLIB_LIBRARY "${ZLIB_PREFIX}/lib/${ZLIB_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  ExternalProject_Add(zlib
          URL http://zlib.net/zlib-${ZLIB_VERSION}.tar.gz
          CMAKE_ARGS ${ZLIB_CMAKE_ARGS}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  include_directories(SYSTEM ${ZLIB_INCLUDE_DIR})
  message(STATUS "ZLib include dir: ${ZLIB_INCLUDE_DIR}")
  message(STATUS "ZLib static library: ${ZLIB_LIBRARY}")

  install(FILES ${ZLIB_LIBRARY} DESTINATION lib)
  install(DIRECTORY ${ZLIB_INCLUDE_DIR}/ DESTINATION include)

  set(OPENSSL_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(OPENSSL_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(OPENSSL_PREFIX "${PROJECT_BINARY_DIR}/external/openssl")
  set(OPENSSL_INCLUDE_DIR "${OPENSSL_PREFIX}/include")
  set(OPENSSL_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}ssl")
  set(CRYPTO_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}crypto")
  set(OPENSSL_LIBRARIES "${OPENSSL_PREFIX}/lib/${OPENSSL_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${OPENSSL_PREFIX}/lib/${CRYPTO_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  ExternalProject_Add(openssl
          DEPENDS zlib
          URL https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz
          BUILD_IN_SOURCE 1
          CONFIGURE_COMMAND ./config -fPIC --prefix=${OPENSSL_PREFIX} --with-zlib-include=${ZLIB_INCLUDE_DIR} --with-zlib-lib=${ZLIB_PREFIX}/lib no-shared no-tests CXX=${CMAKE_CXX_COMPILER} CC=${CMAKE_C_COMPILER}
          BUILD_COMMAND "$(MAKE)"
          INSTALL_COMMAND "$(MAKE)" install
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})
  message(STATUS "OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")
  message(STATUS "OpenSSL static libraries: ${OPENSSL_LIBRARIES}")

  install(FILES ${OPENSSL_LIBRARIES} DESTINATION lib)
  install(DIRECTORY ${OPENSSL_INCLUDE_DIR}/openssl DESTINATION include)

  set(CURL_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(CURL_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(CURL_PREFIX "${PROJECT_BINARY_DIR}/external/curl")
  set(CURL_HOME "${CURL_PREFIX}")
  set(CURL_INCLUDE_DIR "${CURL_PREFIX}/include")
  set(CURL_PREFIX_PATH "${ZLIB_PREFIX}|${OPENSSL_PREFIX}")
  set(CURL_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_CXX_FLAGS=${CURL_CXX_FLAGS}"
          "-DCMAKE_C_FLAGS=${CURL_C_FLAGS}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_INSTALL_PREFIX=${CURL_PREFIX}"
          "-DCMAKE_PREFIX_PATH=${CURL_PREFIX_PATH}"
          "-DCURL_STATICLIB=ON"
          "-DBUILD_CURL_EXE=OFF"
          "-DBUILD_TESTING=OFF"
          "-DENABLE_MANUAL=OFF"
          "-DHTTP_ONLY=ON"
          "-DCURL_CA_PATH=none"
          "-DZLIB_LIBRARY=${ZLIB_LIBRARY}") # Force usage of static library

  set(CURL_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}curl")
  set(CURL_LIBRARY "${CURL_PREFIX}/lib/${CURL_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  string(REGEX REPLACE "\\." "_" CURL_VERSION_STR ${CURL_VERSION})
  ExternalProject_Add(curl
          DEPENDS zlib openssl
          URL https://github.com/curl/curl/releases/download/curl-${CURL_VERSION_STR}/curl-${CURL_VERSION}.tar.gz
          LIST_SEPARATOR |
          CMAKE_ARGS ${CURL_CMAKE_ARGS}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  include_directories(SYSTEM ${CURL_INCLUDE_DIR})
  message(STATUS "Curl include dir: ${CURL_INCLUDE_DIR}")
  message(STATUS "Curl static library: ${CURL_LIBRARY}")

  install(FILES ${CURL_LIBRARY} DESTINATION lib)
  install(DIRECTORY ${CURL_INCLUDE_DIR}/curl DESTINATION include)

  set(AWS_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(AWS_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(AWS_PREFIX "${PROJECT_BINARY_DIR}/external/aws")
  set(AWS_HOME "${AWS_PREFIX}")
  set(AWS_BUILD_PROJECTS "s3")
  set(AWS_INCLUDE_DIR "${AWS_PREFIX}/include")
  set(AWS_PREFIX_PATH "${ZLIB_PREFIX}|${OPENSSL_PREFIX}|${CURL_PREFIX}")
  set(AWS_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_CXX_FLAGS=${AWS_CXX_FLAGS}"
          "-DCMAKE_C_FLAGS=${AWS_C_FLAGS}"
          "-DCMAKE_INSTALL_LIBDIR=lib"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DBUILD_ONLY=${AWS_BUILD_PROJECTS}"
          "-DCMAKE_INSTALL_PREFIX=${AWS_PREFIX}"
          "-DENABLE_TESTING=OFF"
          "-DBUILD_SHARED_LIBS=OFF"
          "-DCMAKE_PREFIX_PATH=${AWS_PREFIX_PATH}"
          "-DZLIB_LIBRARY=${ZLIB_LIBRARY}") # Force usage of static library

  set(AWS_STATIC_CORE_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-core")
  set(AWS_STATIC_S3_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-s3")
  set(AWS_LIBRARIES "${AWS_PREFIX}/lib/${AWS_STATIC_S3_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${AWS_PREFIX}/lib/${AWS_STATIC_CORE_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")

  ExternalProject_Add(awssdk
          DEPENDS curl openssl zlib
          GIT_REPOSITORY "https://github.com/awslabs/aws-sdk-cpp.git"
          GIT_TAG "${AWSSDK_VERSION}"
          GIT_SHALLOW 1
          BUILD_IN_SOURCE true
          LIST_SEPARATOR |
          CMAKE_ARGS ${AWS_CMAKE_ARGS}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  include_directories(SYSTEM ${AWS_INCLUDE_DIR})
  message(STATUS "AWS include dir: ${AWS_INCLUDE_DIR}")
  message(STATUS "AWS static libraries: ${AWS_LIBRARIES}")

  install(FILES ${AWS_LIBRARIES} DESTINATION lib)
  install(DIRECTORY ${AWS_INCLUDE_DIR}/aws DESTINATION include)

  list(APPEND PLASMA_EXTERNAL_STORE_SOURCES "s3_store.cc")
  list(APPEND PLASMA_EXTERNAL_STORE_LIBS ${AWS_LIBRARIES})
endif ()

if (ARROW_PLASMA_REDIS)
  set(CPP_REDIS_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CPP_REDIS_C_FLAGS "${CMAKE_C_FLAGS}")
  set(CPP_REDIS_PREFIX "${PROJECT_BINARY_DIR}/external/cpp_redis")
  set(CPP_REDIS_HOME "${CPP_REDIS_PREFIX}")
  set(CPP_REDIS_INCLUDE_DIR "${CPP_REDIS_PREFIX}/include")
  set(CPP_REDIS_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_CXX_FLAGS=${CPP_REDIS_CXX_FLAGS}"
          "-DCMAKE_C_FLAGS=${CPP_REDIS_C_FLAGS}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_INSTALL_PREFIX=${CPP_REDIS_PREFIX}"
          "-DBUILD_EXAMPLES=OFF"
          "-DBUILD_TESTS=OFF"
          "-DBUILD_SHARED_LIBS=OFF")
  set(CPP_REDIS_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}cpp_redis")
  set(CPP_REDIS_TACOPIE_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}tacopie")
  set(CPP_REDIS_LIBRARY "${CPP_REDIS_PREFIX}/lib/${CPP_REDIS_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(CPP_REDIS_TACOPIE_LIBRARY "${CPP_REDIS_PREFIX}/lib/${CPP_REDIS_TACOPIE_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  ExternalProject_Add(cpp_redis_project
          GIT_REPOSITORY https://github.com/Cylix/cpp_redis.git
          GIT_SUBMODULES
          LIST_SEPARATOR |
          CMAKE_ARGS ${CPP_REDIS_CMAKE_ARGS}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
  include_directories(SYSTEM ${CPP_REDIS_INCLUDE_DIR})
  message(STATUS "cpp_redis include dir: ${CPP_REDIS_INCLUDE_DIR}")
  message(STATUS "cpp_redis static library: ${CPP_REDIS_LIBRARIES}")

  install(FILES ${CPP_REDIS_LIBRARIES} DESTINATION lib)
  install(DIRECTORY ${CPP_REDIS_INCLUDE_DIR}/cpp_redis DESTINATION include)

  add_library(cpp_redis STATIC IMPORTED)
  set_property(TARGET cpp_redis PROPERTY IMPORTED_LOCATION ${CPP_REDIS_LIBRARY})
  add_dependencies(cpp_redis cpp_redis_project)

  add_library(tacopie STATIC IMPORTED)
  set_property(TARGET tacopie PROPERTY IMPORTED_LOCATION ${CPP_REDIS_TACOPIE_LIBRARY})
  add_dependencies(tacopie cpp_redis_project)

  list(APPEND PLASMA_EXTERNAL_STORE_SOURCES "redis_store.cc")
  list(APPEND PLASMA_EXTERNAL_STORE_LIBS cpp_redis tacopie)
endif ()

if (PLASMA_EXTRA_EXTERNAL_STORE_SOURCES)
  list(APPEND PLASMA_EXTERNAL_STORE_SOURCES ${PLASMA_EXTRA_EXTERNAL_STORE_SOURCES})
endif ()

if (PLASMA_EXTERNAL_STORE_LIBDIRS)
  link_directories(${PLASMA_EXTERNAL_STORE_LIBDIRS})
endif ()

# We use static libraries for the plasma_store_server executable so that it can
# be copied around and used in different locations.
add_executable(plasma_store_server ${PLASMA_EXTERNAL_STORE_SOURCES} store.cc)
target_link_libraries(plasma_store_server plasma_static ${PLASMA_STATIC_LINK_LIBS} ${PLASMA_EXTERNAL_STORE_LIBS})
add_dependencies(plasma plasma_store_server)

if (ARROW_RPATH_ORIGIN)
  if (APPLE)
    set(_lib_install_rpath "@loader_path")
  else()
    set(_lib_install_rpath "\$ORIGIN")
  endif()
  set_target_properties(plasma_store_server PROPERTIES
      INSTALL_RPATH ${_lib_install_rpath})
endif()

install(FILES
  common.h
  compat.h
  client.h
  events.h
  test-util.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/plasma")

# Plasma store
set_target_properties(plasma_store_server PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS plasma_store_server
  ${INSTALL_IS_OPTIONAL}
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# pkg-config support
ARROW_ADD_PKG_CONFIG("plasma")

if(ARROW_PLASMA_JAVA_CLIENT)
  # Plasma java client support
  find_package(JNI REQUIRED)
  # add jni support
  include_directories(${JAVA_INCLUDE_PATH})
  include_directories(${JAVA_INCLUDE_PATH2})
  if (JNI_FOUND)
    message (STATUS "JNI_INCLUDE_DIRS = ${JNI_INCLUDE_DIRS}")
    message (STATUS "JNI_LIBRARIES = ${JNI_LIBRARIES}")
  else()
    message (WARNING "Could not find JNI")
  endif()

  add_compile_options("-I$ENV{JAVA_HOME}/include/")
  if(WIN32)
    add_compile_options("-I$ENV{JAVA_HOME}/include/win32")
  elseif(APPLE)
    add_compile_options("-I$ENV{JAVA_HOME}/include/darwin")
  else() # linux
    add_compile_options("-I$ENV{JAVA_HOME}/include/linux")
  endif()

  include_directories("${CMAKE_CURRENT_LIST_DIR}/lib/java")

  file(GLOB PLASMA_LIBRARY_EXT_java_SRC
    lib/java/*.cc lib/*.cc)
  add_library(plasma_java SHARED
    ${PLASMA_LIBRARY_EXT_java_SRC})

  if(APPLE)
    target_link_libraries(plasma_java plasma_shared ${PLASMA_LINK_LIBS} "-undefined dynamic_lookup" -Wl,-force_load,${FLATBUFFERS_STATIC_LIB} ${FLATBUFFERS_STATIC_LIB} ${PTHREAD_LIBRARY})
  else(APPLE)
    target_link_libraries(plasma_java plasma_shared ${PLASMA_LINK_LIBS} -Wl,--whole-archive ${FLATBUFFERS_STATIC_LIB} -Wl,--no-whole-archive ${FLATBUFFERS_STATIC_LIB} ${PTHREAD_LIBRARY})
  endif(APPLE)
endif()
#######################################
# Unit tests
#######################################

# Adding unit tests part of the "arrow" portion of the test suite
function(ADD_PLASMA_TEST REL_TEST_NAME)
  set(options)
  set(one_value_args)
  set(multi_value_args)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  ADD_TEST_CASE(${REL_TEST_NAME}
    PREFIX "plasma"
    LABELS "plasma-tests"
    ${ARG_UNPARSED_ARGUMENTS})
endfunction()

ADD_PLASMA_TEST(test/serialization_tests
  EXTRA_LINK_LIBS plasma_shared ${PLASMA_LINK_LIBS})
ADD_PLASMA_TEST(test/client_tests
  EXTRA_LINK_LIBS plasma_shared ${PLASMA_LINK_LIBS}
  EXTRA_DEPENDENCIES plasma_store_server)
ADD_PLASMA_TEST(test/external_store_tests
  EXTRA_LINK_LIBS plasma_shared ${PLASMA_LINK_LIBS}
  EXTRA_DEPENDENCIES plasma_store_server)
