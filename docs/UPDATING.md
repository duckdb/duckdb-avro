# Extension updating 
When cloning this template, the target version of DuckDB should be the latest stable release of DuckDB. However, there 
will inevitably come a time when a new DuckDB is released and the extension repository needs updating. This process goes
as follows:

- Bump submodules
  - `./duckdb` should be set to latest tagged release
  - `./extension-ci-tools` should be set to updated branch corresponding to latest DuckDB release. So if you're building for DuckDB `v1.1.0` there will be a branch in `extension-ci-tools` named `v1.1.0` to which you should check out. 
- Bump versions in `./github/workflows`
  - `duckdb_version` input in `duckdb-stable-build` job in `MainDistributionPipeline.yml` should be set to latest tagged release
  - `duckdb_version` input in `duckdb-stable-deploy` job in `MainDistributionPipeline.yml` should be set to latest tagged release
  - the reusable workflow `duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml` for the `duckdb-stable-build` job should be set to latest tagged release

# API changes
This extension is written against DuckDB's stable C++ API (`duckdb_cpp.hpp`, in `duckdb/tools/cpp`), which is built
on top of the V2 C API, rather than against the internal C++ API of DuckDB. It does not include any of DuckDB's
internal headers.

Some of the parts of the stable C++ API it relies on are still marked as *unstable*: the multi-file function behind
`read_avro`, the per-block batch claiming of its scan, the field ids and metadata it reports for a file, and the
statistics of `COPY ... TO`. Because of this, the extension is built with the `C_STRUCT_UNSTABLE` ABI, which pins a
build to the exact DuckDB version it was built against - just like an extension built against the internal C++ API.
Once those parts of the API are stabilized, the extension can be built against a stable API version instead.

When updating the DuckDB target version, check the git history of `tools/cpp/duckdb_cpp.hpp` for changes to the
parts of the API that are still unstable.
