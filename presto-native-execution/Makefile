.PHONY: all cmake build clean debug release unittest bolt-conan-recipe

BUILD_VERSION ?= main
BUILD_USER ?=
BUILD_CHANNEL ?=

BUILD_BASE_DIR=_build
BUILD_TYPE=Release

ENABLE_HDFS ?= True
ENABLE_ASAN ?= False
ENABLE_JEMALLOC ?= True
ENABLE_JEMALLOC_PROF ?= False

CONAN_OPTIONS := BUILD_VERSION=${BUILD_VERSION}
CONAN_OPTIONS += BUILD_USER=${BUILD_USER}
CONAN_OPTIONS += BUILD_CHANNEL=${BUILD_CHANNEL}
CONAN_OPTIONS += ENABLE_HDFS=${ENABLE_HDFS}
CONAN_OPTIONS += ENABLE_ASAN=${ENABLE_ASAN}

JEMALLOC_OPTIONS = -o enable_jemalloc=${ENABLE_JEMALLOC} -o enable_jemalloc_prof=${ENABLE_JEMALLOC_PROF}

all: release			#: Build the release version

clean:					#: Delete all build artifacts
	rm -rf $(BUILD_BASE_DIR)

print_param:			#: Print build param
	echo ${CONAN_OPTIONS}

bolt-conan-recipe:
    set -x && \
    git clone --branch ${BUILD_VERSION} --single-branch --depth 1 https://github.com/bytedance/bolt.git bolt && \
    cd bolt && \
    conan remove -c bolt && \
    conan export . --name=bolt --version=${BUILD_VERSION} && \
    cd -

conan_build: print_param bolt-conan-recipe
	if [ ! -d "_build" ]; then \
		mkdir _build; \
	fi; \
	git rev-parse HEAD && \
	mkdir -p _build/${BUILD_TYPE} && \
	rm -f _build/${BUILD_TYPE}/CMakeCache.txt && \
	cd _build/${BUILD_TYPE} && \
	echo " -o *:enable_hdfs=${ENABLE_HDFS} \
	-o *:enable_asan=${ENABLE_ASAN} \
	$(JEMALLOC_OPTIONS)" > new_conan.options && \
    set -x && \
    if [ -f conan.options ] && [ -f ../.build_type ] && cmp -s new_conan.options conan.options && [ "`cat ../.build_type`" = "${BUILD_TYPE}" ]; then \
	  echo "Conan options and build type unchanged; preserving CMakeCache.txt"; \
	else \
	  echo "Conan options and build type changed! deleting CMakeCache.txt"; \
	  rm -f CMakeCache.txt; \
	fi && \
    mv new_conan.options conan.options && \
    echo ${BUILD_TYPE} > ../.build_type && \
    read ALL_CONAN_OPTIONS < conan.options && \
	conan graph info ../..  $${ALL_CONAN_OPTIONS} --format=html > presto.conan.graph.html  && \
	conan install ../.. --name=presto_server --version=${BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} \
	    -s build_type=${BUILD_TYPE} --build=missing \
        -s build_type=$${DEPENDENCY_BUILD_TYPE:-${BUILD_TYPE}} \
        $${ALL_CONAN_OPTIONS} && \
	NUM_THREADS=$(NUM_THREADS) \
    conan build ../.. --name=presto_server --version=${BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} \
	    -s build_type=${BUILD_TYPE} --build=missing \
        -s build_type=$${DEPENDENCY_BUILD_TYPE:-${BUILD_TYPE}} \
        $${ALL_CONAN_OPTIONS} && \
	cd -

all: 		#: Build the release version
	$(MAKE) release

export_base:
	cd _build/${BUILD_TYPE} && \
	read ALL_CONAN_OPTIONS < conan.options && \
	conan export-pkg --name=bolt-internal --version=${BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} \
	-s build_type=${BUILD_TYPE} $${ALL_CONAN_OPTIONS} ../.. && \
	cd -

export_debug:
	$(MAKE) export_base BUILD_TYPE=Debug

export_release:
	$(MAKE) export_base BUILD_TYPE=Release

install:
	$(MAKE) export_base BUILD_TYPE=$(shell cat _build/.build_type)

debug:      #: Build with debugging symbols
	$(MAKE) conan_build BUILD_TYPE=Debug

release:  	#: Build the release version
	$(MAKE) conan_build BUILD_TYPE=Release

presto_protocol:		#: Build the presto_protocol serde library
	cd presto_cpp/presto_protocol; $(MAKE) presto_protocol

unittest_release: release_with_test		#: Build with release and run unit tests
	ctest --test-dir $(BUILD_BASE_DIR)/Release --timeout 7200 -j $(NUM_THREADS) --output-on-failure

unittest: debug_with_test			    #: Build with debug and run unit tests
	ctest --test-dir $(BUILD_BASE_DIR)/Debug --timeout 7200 -j $(NUM_THREADS) --output-on-failure
