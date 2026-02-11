import os

from conan import ConanFile
from conan.tools import files
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.env import Environment, VirtualBuildEnv, VirtualRunEnv

class PrestoServeroConan(ConanFile):
    description = """ Presto Cpp Worker"""

    name = "presto_server"

    settings = "os", "arch", "compiler", "build_type"

    options = {
        "simd_level" : ["default", "sse4_2", "avx2", "avx512", "neon", ],
        "enable_s3": [True, False],
        "enable_hdfs": [True, False],
        "enable_parquet": [True, False],
        "enable_asan": [True, False],
        "enable_jemalloc": [True, False],
        "enable_jemalloc_prof": [True, False],
        "enable_testing": [True, False],
    }
    default_options = {
        "simd_level": "avx2",
        "enable_s3": False,
        "enable_hdfs": True,
        "enable_parquet": True,
        "enable_asan" : False,
        "enable_jemalloc": True,
        "enable_jemalloc_prof": False,
        "enable_testing": False,
    }

    FB_VERSION = "2022.10.31.00"

    def requirements(self):
        #if self.channel is not None:
        #    self.requires(f"bolt/{self.version}@{self.user}/{self.channel}")
        #else:
        #    self.requires(f"bolt/{self.version}")
        self.requires('bolt/main', transitive_headers=True, transitive_libs=True)

        self.requires(f"folly/{self.FB_VERSION}", transitive_headers=True, transitive_libs=True)
        self.requires(f'proxygen/{self.FB_VERSION}', transitive_headers=True, transitive_libs=True, force=True)
        self.requires("libsodium/1.0.19", transitive_headers=True, transitive_libs=True)

        if self.options.enable_jemalloc:
            self.requires("jemalloc/5.3.0")

    def build_requirements(self):
        self.tool_requires("cmake/3.25.3")
        self.tool_requires("ninja/1.11.1")
        self.tool_requires(f'fbthrift/{self.FB_VERSION}')
        if self.options.get_safe("enable_testing"):
            self.test_requires("gtest/1.17.0")

    def configure(self):
        bolt = "bolt"
        self.options[bolt].enable_crc = True
        self.options[bolt].spark_compatible = False
        self.options[bolt].enable_s3 = self.options.enable_s3
        self.options[bolt].enable_hdfs = self.options.enable_hdfs
        self.options[bolt].enable_parquet = self.options.enable_parquet
        self.options[bolt].enable_asan = self.options.enable_asan

        jemalloc = "jemalloc"
        if self.options.enable_jemalloc and self.options.enable_jemalloc_prof:
            self.options[jemalloc].enable_prof = True

    def layout(self):
        cmake_layout(self, build_folder='_build')

    def generate(self):
        build_env = VirtualBuildEnv(self)
        build_env.generate()

        run_env = VirtualRunEnv(self)
        run_env.generate()

        tc = CMakeToolchain(self, generator="Ninja")

        tc.cache_variables["PRESTO_ENABLE_TESTING"] = "ON" if self.options.enable_testing else "OFF" 

        tc.cache_variables["PRESTO_ENABLE_HDFS"]="ON" if self.options.enable_hdfs else "OFF"
        tc.cache_variables["PRESTO_ENABLE_S3"]="ON" if self.options.enable_s3 else "OFF"

        tc.cache_variables["PRESTO_ENABLE_PARQUET"]="ON" if self.options.enable_parquet else "OFF"

        if self.options.enable_jemalloc:
            tc.cache_variables["PRESTO_ENABLE_JEMALLOC"]="ON"

        cxx_flag = ""
        if str(self.settings.arch) in ['x86', 'x86_64']:
            cxx_flag = f" -m{self.options.simd_level} "
            if self.options.simd_level != "avx512":
                cxx_flag += " -mno-avx512f "
        elif str(self.settings.arch) in ['armv8']:
            # enable NEON, CRC
            cxx_flag += "-march=armv8.1-a"
        elif str(self.settings.arch) in ['armv9']:
            # gcc 12+ https://www.phoronix.com/news/GCC-12-ARMv9-march-armv9-a
            cxx_flag += "-march=armv9-a"

        if self.options.enable_asan:
            cxx_flag += " -fsanitize=address -fno-omit-frame-pointer "

        tc.cache_variables["CMAKE_CXX_FLAGS"] = cxx_flag
        tc.cache_variables["CMAKE_C_FLAGS"] = cxx_flag
        
        tc.generate()

        # generate conantoolchain.cmake & xxx-config.cmake
        deps = CMakeDeps(self)
        deps.set_property("flex", "cmake_find_mode", "config") 
        deps.set_property("fbthrift", "cmake_find_mode", "config") 
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.install()

    def _print_dependencies_options(self):
        # conaninfo.txt
        options = []
        for req in self.requires:
            for key, value in self.options[req].items():
                options.append(f"{req}:{key}={value}")
        self.output.info("\n====Presto Server Dependecies and options ==========\n")
        self.output.info("\n".join(sorted(options)))
        self.output.info("\n=============================================\n")
