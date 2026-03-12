#!/usr/bin/env python3
"""
SparkShell - Standalone Python class to download, build, start, and interact with SparkApp server.

The SparkShell automatically handles all setup, downloading, and building when you call start().
You only need to provide the source location and call start() - everything else is automatic!

Features:
- Automatic caching: Builds are cached in ~/.sparkshell_cache for faster subsequent startups
- Force refresh: Use start(force_refresh=True) to bypass cache and force fresh build

Usage:
    # Basic usage with context manager (automatic setup, build, and start)
    from spark_shell import SparkShell

    with SparkShell(source=".") as shell:
        result = shell.execute_sql("SELECT 1 as id")
        print(result)

    # Manual start (still automatic setup and build)
    shell = SparkShell(source=".")
    shell.start()  # Uses cached build if available
    result = shell.execute_sql("SELECT 1 as id")
    shell.shutdown()

    # Force fresh build (bypass cache)
    shell = SparkShell(source=".")
    shell.start(force_refresh=True)  # Forces fresh download and rebuild
    result = shell.execute_sql("SELECT 1 as id")
    shell.shutdown()

    # With configuration classes
    from spark_shell import SparkShell, UCConfig, OpConfig, SparkConfig, UnityCatalogSourceConfig

    uc_config = UCConfig(uri="http://localhost:8081", token="my-token", catalog="unity", schema="default")
    op_config = OpConfig(verbose=True, startup_timeout=120, cleanup_on_exit=True)
    spark_config = SparkConfig(configs={"spark.executor.memory": "2g"})
    # Use FGAC-enabled UC fork (default settings)
    uc_source_config = UnityCatalogSourceConfig()

    with SparkShell(source=".", uc_config=uc_config, op_config=op_config,
                    spark_config=spark_config, uc_source_config=uc_source_config) as shell:
        result = shell.execute_sql("SELECT * FROM my_table")
        print(result)
"""

import os
import sys
import time
import shutil
import tempfile
import subprocess
import json
import requests
import hashlib
from pathlib import Path
from typing import Optional, Union, Tuple
from dataclasses import dataclass, field
from datetime import datetime

# Shared debug log file - same as Delta's FGACDebugLog.scala
FGAC_DEBUG_LOG = "/tmp/fgac_debug.log"


class _TeeOutput:
    """Writes to original stream and to log files. Buffers until add_file() so a later-added file gets full log."""

    def __init__(self, stream, log_files: list):
        self._stream = stream
        self._log_files = list(log_files)
        self._buffer: Optional[list] = []  # buffer output until add_file() so the new file gets content from start

    def add_file(self, f):
        if self._buffer is not None:
            try:
                f.write("".join(self._buffer))
                f.flush()
            except (OSError, ValueError):
                pass
            self._buffer = None
        self._log_files.append(f)

    def write(self, data):
        self._stream.write(data)
        if self._buffer is not None:
            self._buffer.append(data)
        for f in self._log_files:
            try:
                f.write(data)
                f.flush()
            except (OSError, ValueError):
                pass

    def flush(self):
        self._stream.flush()
        for f in self._log_files:
            try:
                f.flush()
            except (OSError, ValueError):
                pass

    def isatty(self):
        return getattr(self._stream, "isatty", lambda: False)()


def fgac_log(component: str, message: str):
    """Write debug message to shared FGAC debug log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open(FGAC_DEBUG_LOG, "a") as f:
        f.write(f"[{timestamp}] [{component}] {message}\n")
        f.flush()
        os.fsync(f.fileno())  # Force write to disk


@dataclass
class UCConfig:
    """Unity Catalog configuration."""
    uri: Optional[str] = None
    token: Optional[str] = None
    catalog: str = "unity"
    schema: Optional[str] = None


@dataclass
class OpConfig:
    """Operational configuration for SparkShell lifecycle."""
    verbose: bool = True
    auto_start: bool = True
    cleanup_on_exit: bool = True
    startup_timeout: int = 60
    build_timeout: int = 300
    java17_fallback_path: Optional[str] = None


@dataclass
class SparkConfig:
    """Spark configuration settings."""
    configs: dict = field(default_factory=dict)


@dataclass
class BuildConfig:
    """Global build configuration that spans all source projects (Delta, UC, SparkShell)."""
    scala_version: str = "2.13.15"
    hadoop_version: str = ""

    def get_cache_key_component(self) -> str:
        """Get a unique string for cache key computation."""
        return f"scala{self.scala_version}_hadoop{self.hadoop_version or 'default'}"


@dataclass
class DeltaConfig:
    """Delta Lake dependency configuration."""
    source_repo: str
    source_branch: str = "master"
    spark_version: str = "4.0.1"
    spark_dep_version: str = "4.0.0"
    source_dir: Optional[str] = None

    def __post_init__(self):
        """Validate configuration."""
        if self.source_dir:
            return
        if not self.source_repo.startswith("http"):
            raise ValueError(
                f"source_repo must be a URL (starting with 'http'), got: {self.source_repo}"
            )

    def get_cache_key_component(self) -> str:
        """Get a unique string for cache key computation."""
        if self.source_dir:
            source_dir = str(Path(self.source_dir).expanduser().resolve())
            return f"delta_local_{source_dir}_{self.spark_version}"
        return f"delta_{self.source_repo}_{self.source_branch}_{self.spark_version}"


@dataclass
class UnityCatalogSourceConfig:
    """Unity Catalog source configuration for building from a fork with FGAC support."""
    source_repo: str = "https://github.com/murali-db/unitycatalog"
    source_branch: str = "fgac-fix"
    source_dir: Optional[str] = None

    def __post_init__(self):
        """Validate configuration."""
        if self.source_dir:
            return
        if not self.source_repo.startswith("http"):
            raise ValueError(
                f"source_repo must be a URL (starting with 'http'), got: {self.source_repo}"
            )

    def get_cache_key_component(self) -> str:
        """Get a unique string for cache key computation."""
        if self.source_dir:
            source_dir = str(Path(self.source_dir).expanduser().resolve())
            return f"uc_local_{source_dir}"
        return f"uc_{self.source_repo}_{self.source_branch}"


class SparkShell:
    """
    A standalone class to manage SparkApp server lifecycle and SQL execution.
    
    Features:
    - Download from GitHub or copy from local directory
    - Build the assembly JAR automatically
    - Start/stop the server
    - Execute SQL commands and get results
    - Context manager support for automatic cleanup
    """
    
    def __init__(
        self,
        source: str,
        port: int = 8080,
        temp_dir: Optional[str] = None,
        uc_config: Optional[UCConfig] = None,
        op_config: Optional[OpConfig] = None,
        spark_config: Optional[SparkConfig] = None,
        delta_config: Optional[DeltaConfig] = None,
        uc_source_config: Optional[UnityCatalogSourceConfig] = None,
        build_config: Optional[BuildConfig] = None
    ):
        """
        Initialize SparkShell.

        Args:
            source: GitHub URL or local directory path containing SparkApp code
            port: Port for the server (default: 8080)
            temp_dir: Custom temp directory (default: system temp)
            uc_config: Unity Catalog configuration (UCConfig object)
            op_config: Operational configuration (OpConfig object)
            spark_config: Spark configuration (SparkConfig object)
            delta_config: Delta Lake configuration (DeltaConfig object)
            uc_source_config: Unity Catalog source config for FGAC support (UnityCatalogSourceConfig object)
            build_config: Global build configuration (BuildConfig object)
        """
        self.source = source
        self.port = port
        self.temp_dir = temp_dir

        # Initialize configurations with defaults or provided config objects
        self.op_config = op_config or OpConfig()
        self.spark_config = spark_config or SparkConfig()
        self.uc_config = uc_config or UCConfig()
        self.build_config = build_config or BuildConfig()
        self.delta_config = delta_config or DeltaConfig(
            source_repo="https://github.com/delta-io/delta",
            source_branch="master"
        )
        # UC source config - defaults to FGAC-enabled fork
        self.uc_source_config = uc_source_config or UnityCatalogSourceConfig()

        # Configure Unity Catalog if URI and token are provided
        if self.uc_config.uri and self.uc_config.token:
            # Register the catalog type
            self.spark_config.configs[f"spark.sql.catalog.{self.uc_config.catalog}"] = "io.unitycatalog.spark.UCSingleCatalog"
            self.spark_config.configs[f"spark.sql.catalog.{self.uc_config.catalog}.uri"] = self.uc_config.uri
            self.spark_config.configs[f"spark.sql.catalog.{self.uc_config.catalog}.token"] = self.uc_config.token
            self.spark_config.configs["spark.sql.defaultCatalog"] = self.uc_config.catalog

        # Runtime state
        self.work_dir: Optional[Path] = None
        self.process: Optional[subprocess.Popen] = None
        self.jar_path: Optional[Path] = None
        self.is_ready = False
        self._log_handle = None
        self._log_thread = None

        # Tee: all print output is also written to a log file
        self._original_stdout: Optional[object] = None
        self._tee_instance: Optional[_TeeOutput] = None

        # API base URL
        self.base_url = f"http://localhost:{self.port}"

        # Debug: print config summary so we know exactly what is happening
        self._debug("=== SparkShell config ===")
        self._debug("  source:", self.source)
        self._debug("  port:", self.port)
        self._debug("  work_dir (initial):", self.work_dir)
        self._debug("  Delta repo:", self.delta_config.source_repo)
        self._debug("  Delta branch:", self.delta_config.source_branch)
        self._debug("  Delta source_dir:", self.delta_config.source_dir)
        self._debug("  Delta spark_version:", self.delta_config.spark_version)
        self._debug("  Scala version:", self.build_config.scala_version)
        self._debug("  UC source repo:", self.uc_source_config.source_repo)
        self._debug("  UC source branch:", self.uc_source_config.source_branch)
        self._debug("  UC source_dir:", self.uc_source_config.source_dir)
        self._debug("  verbose:", self.op_config.verbose)
        self._debug("  startup_timeout:", self.op_config.startup_timeout)
        self._debug("  build_timeout:", self.op_config.build_timeout)
        self._debug("  cache key hash:", self._get_source_hash())
        self._debug("  cache dir:", self._get_cache_dir())
        self._debug("=========================")

    def _debug(self, msg: str, *args):
        """Print debug message (always on so we know exactly what is happening)."""
        parts = [str(msg)] + [str(a) for a in args]
        print("[SparkShell] DEBUG:", " ".join(parts))

    def _get_source_hash(self) -> str:
        """
        Compute a hash of the source to use as cache key.
        For local paths, hash the absolute path.
        For URLs, hash the URL itself.
        Includes Delta and UC configuration to prevent cache collision.
        """
        source_str = str(Path(self.source).resolve()) if not self.source.startswith("http") else self.source
        build_str = self.build_config.get_cache_key_component()
        delta_str = self.delta_config.get_cache_key_component()
        uc_str = self.uc_source_config.get_cache_key_component()
        combined = f"{source_str}_{build_str}_{delta_str}_{uc_str}"
        source_hash = hashlib.sha256(combined.encode()).hexdigest()[:16]

        if self.op_config.verbose:
            print(f"[SparkShell] Cache key computation:")
            print(f"  Source: {self.source}")
            print(f"  Normalized: {source_str}")
            print(f"  Build config: {build_str}")
            print(f"  Delta config: {delta_str}")
            print(f"  UC config: {uc_str}")
            print(f"  Combined: {combined}")
            print(f"  Cache key (hash): {source_hash}")

        return source_hash

    def _get_cache_dir(self) -> Path:
        """Get the cache directory for this source."""
        cache_base = Path.home() / ".sparkshell_cache"
        cache_base.mkdir(parents=True, exist_ok=True)
        cache_dir = cache_base / self._get_source_hash()

        if self.op_config.verbose:
            print(f"[SparkShell] Cache directory: {cache_dir}")

        return cache_dir

    def _has_cached_build(self) -> bool:
        """Check if a cached build exists for this source."""
        cache_dir = self._get_cache_dir()
        jar_path = cache_dir / "target" / "scala-2.13" / "sparkshell.jar"
        has_cache = jar_path.exists()
        self._debug("_has_cached_build: cache_dir=", cache_dir, "jar exists=", has_cache)

        if self.op_config.verbose:
            print(f"[SparkShell] Cache status:")
            print(f"  Cache directory: {cache_dir}")
            print(f"  Expected JAR: {jar_path}")
            print(f"  Cache exists: {'Yes' if has_cache else 'No'}")

        return has_cache

    def _use_cached_build(self):
        """Use the cached build instead of building from scratch."""
        cache_dir = self._get_cache_dir()
        self._debug("_use_cached_build: cache_dir=", cache_dir)
        print(f"[SparkShell] Using cached build from: {cache_dir}")

        # Set work_dir to cache directory
        self.work_dir = cache_dir

        # Set jar_path
        self.jar_path = cache_dir / "target" / "scala-2.13" / "sparkshell.jar"
        self._debug("  jar_path=", self.jar_path)

        if not self.jar_path.exists():
            raise RuntimeError(f"Cached JAR not found at: {self.jar_path}")

        print(f"[SparkShell] Using cached JAR: {self.jar_path}")

    def _ensure_sbtopts(self):
        """
        Ensure .sbtopts file is present in work_dir.
        This must be called after work_dir is finalized (after cache decision).
        Writes embedded .sbtopts content directly to make spark_shell.py standalone.
        """
        if not self.work_dir:
            raise RuntimeError("work_dir must be set before calling _ensure_sbtopts")

        sbtopts_dest = self.work_dir / ".sbtopts"

        # Embedded .sbtopts content - SBT JVM memory settings
        sbtopts_content = """-J-Xmx2G
-J-Xms1G
-J-XX:+UseG1GC
-J-XX:MaxMetaspaceSize=1G
"""

        # Write .sbtopts file
        with open(sbtopts_dest, 'w') as f:
            f.write(sbtopts_content)

        if self.op_config.verbose:
            print(f"[SparkShell] Created .sbtopts at {sbtopts_dest}")

    def _cache_build(self):
        """Cache the current build for future reuse."""
        self._debug("_cache_build: work_dir=", self.work_dir, "jar_path=", self.jar_path)
        if not self.work_dir or not self.jar_path:
            self._debug("  skip: no work_dir or jar_path")
            return

        cache_dir = self._get_cache_dir()

        # If we're already using the cache directory, no need to copy
        if self.work_dir == cache_dir:
            self._debug("  skip: already in cache dir")
            return

        self._debug("  copying work_dir to cache_dir:", cache_dir)
        print(f"[SparkShell] Caching build to: {cache_dir}")

        # Remove old cache if it exists
        if cache_dir.exists():
            shutil.rmtree(cache_dir)

        # Copy entire work directory to cache
        shutil.copytree(self.work_dir, cache_dir)

        print(f"[SparkShell] Build cached successfully")

    def _setup_delta(self) -> Path:
        """
        Clone Delta repository and checkout specified branch.

        Returns:
            Path to Delta directory
        """
        # Local-dev mode: use pre-existing local Delta source directly.
        if self.delta_config.source_dir:
            local_delta_dir = Path(self.delta_config.source_dir).expanduser().resolve()
            self._debug("_setup_delta: source mode=local_dir path=", local_delta_dir)
            if not local_delta_dir.exists():
                raise RuntimeError(
                    f"Configured local Delta directory does not exist: {local_delta_dir}\n"
                    f"Set DeltaConfig.source_dir to a valid path."
                )
            if not (local_delta_dir / "build.sbt").exists():
                raise RuntimeError(
                    f"Local Delta directory does not contain build.sbt: {local_delta_dir}"
                )
            print(f"[SparkShell] Delta source mode: local_dir ({local_delta_dir})")
            return local_delta_dir

        delta_dir = self.work_dir / "delta"
        self._debug("_setup_delta: work_dir=", self.work_dir, "delta_dir=", delta_dir)
        print("[SparkShell] Delta source mode: git_clone")

        if delta_dir.exists():
            self._debug("  Delta dir already exists, skipping clone")
            if self.op_config.verbose:
                print(f"[SparkShell] Delta directory already exists: {delta_dir}")
            return delta_dir

        self._debug("  Cloning from", self.delta_config.source_repo)
        print(f"[SparkShell] Cloning Delta from {self.delta_config.source_repo}...")

        try:
            self._run_command(
                ["git", "clone", self.delta_config.source_repo, "delta"],
                cwd=self.work_dir,
                timeout=300,
                check=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to clone Delta repository from {self.delta_config.source_repo}\n"
                f"Error: {e}\n"
                f"Please check that the repository URL is correct and accessible."
            )

        self._debug("  Checking out branch:", self.delta_config.source_branch)
        print(f"[SparkShell] Checking out branch: {self.delta_config.source_branch}")

        try:
            self._run_command(
                ["git", "checkout", self.delta_config.source_branch],
                cwd=delta_dir,
                timeout=30,
                check=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to checkout branch '{self.delta_config.source_branch}'\n"
                f"Error: {e}\n"
                f"Please verify the branch exists in the repository."
            )

        # Verify build.sbt exists
        build_sbt = delta_dir / "build.sbt"
        cross_spark_file = delta_dir / "project" / "CrossSparkVersions.scala"
        self._debug("  build.sbt exists:", build_sbt.exists())
        self._debug("  CrossSparkVersions.scala exists:", cross_spark_file.exists())
        if not build_sbt.exists():
            raise RuntimeError(
                f"Delta repository does not contain build.sbt\n"
                f"Directory: {delta_dir}\n"
                f"This may not be a valid Delta Lake repository."
            )

        self._debug("_setup_delta complete:", delta_dir)
        print(f"[SparkShell] Delta repository ready: {delta_dir}")
        return delta_dir

    def _get_maven_local_env(self) -> dict:
        """
        Build env for all SBT builds (Delta, UC, SparkShell).

        Optionally overrides JAVA_HOME when java17_fallback_path is set (for boxes
        where the system JDK is too old for Spark 4.x). Also sets SBT_OPTS so that
        publishM2 and resolution use a temp Maven repo under work_dir/m2_repo.
        """
        env: dict[str, str] = {}

        if self.op_config.java17_fallback_path:
            env["JAVA_HOME"] = self.op_config.java17_fallback_path
            env["PATH"] = os.path.join(env["JAVA_HOME"], "bin") + ":" + os.environ.get("PATH", "")

        if getattr(self, "_m2_repo", None) and self._m2_repo:
            m2 = str(Path(self._m2_repo).resolve())
            opts = os.environ.get("SBT_OPTS", "")
            if "-Dmaven.repo.local=" not in opts:
                opts = f"{opts} -Dmaven.repo.local={m2}".strip()
            env["SBT_OPTS"] = opts

        return env

    def _patch_sbt_for_temp_repo(self, project_dir: Path):
        """
        Patch a project's build/sbt so all publishM2 and resolution uses the temp repo.

        Many build/sbt scripts (from Delta, SparkShell) unconditionally overwrite SBT_OPTS,
        destroying -Dmaven.repo.local set by _get_maven_local_env(). Additionally, they set
        -Dsbt.override.build.repos=true which ignores build.sbt resolvers.

        This method fixes both issues for any project dir that has build/sbt:
        1. Patches build/sbt to append to SBT_OPTS instead of overwriting.
        2. Injects the temp Maven repo into sbt-config/repositories.

        Only patches directories under work_dir (temp copies we own). User-provided
        source_dir paths are never modified; a warning is emitted if a clobbering
        pattern is detected there.
        """
        if not getattr(self, "_m2_repo", None) or not self._m2_repo:
            return
        if not self.work_dir:
            return

        project_dir = Path(project_dir).resolve()
        work_dir_resolved = Path(self.work_dir).resolve()

        # Safety: never modify files outside our temp work_dir.
        if not str(project_dir).startswith(str(work_dir_resolved)):
            sbt_script = project_dir / "build" / "sbt"
            if sbt_script.exists():
                content = sbt_script.read_text()
                if 'export SBT_OPTS="-D' in content and '${SBT_OPTS}' not in content:
                    print(
                        f"[SparkShell] WARNING: {sbt_script} overwrites SBT_OPTS "
                        f"(clobbers -Dmaven.repo.local). Cannot patch external source dir. "
                        f"Build may fall back to ~/.m2 for this project."
                    )
            self._debug("_patch_sbt_for_temp_repo: skipping external dir", project_dir)
            return

        m2_url = f"file://{self._m2_repo.resolve()}"

        # 1. Patch build/sbt: preserve existing SBT_OPTS
        sbt_script = project_dir / "build" / "sbt"
        if sbt_script.exists():
            content = sbt_script.read_text()
            old = 'export SBT_OPTS="-D'
            new = 'export SBT_OPTS="${SBT_OPTS} -D'
            if old in content and new not in content:
                content = content.replace(old, new)
                sbt_script.write_text(content)
                self._debug("_patch_sbt_for_temp_repo: patched SBT_OPTS in", sbt_script)

        # 2. Patch sbt-config/repositories: add temp repo as highest-priority Maven source
        repos_config = project_dir / "build" / "sbt-config" / "repositories"
        if repos_config.exists():
            content = repos_config.read_text()
            if m2_url not in content:
                patched = content.replace(
                    "[repositories]\n",
                    f"[repositories]\n  build-local-maven: {m2_url}\n",
                    1,
                )
                repos_config.write_text(patched)
                self._debug("_patch_sbt_for_temp_repo: added", m2_url, "to", repos_config)

        if self.op_config.verbose:
            print(f"[SparkShell] Patched SBT config in {project_dir} for temp Maven repo")

    def _build_delta(self, delta_dir: Path):
        """
        Build Delta Lake and publish to local Maven repository.

        All artifacts (including delta-kernel-unitycatalog) are published to the
        temp Maven repo (work_dir/m2_repo) so the build is reproducible.
        """
        self._debug("_build_delta: delta_dir=", delta_dir)
        print("[SparkShell] Building Delta Lake from source...")
        print("[SparkShell] This may take 10+ minutes on first run...")

        sbt_script = delta_dir / "build" / "sbt"
        self._debug("  sbt_script=", sbt_script, "exists=", sbt_script.exists())
        if not sbt_script.exists():
            raise FileNotFoundError(f"Delta SBT script not found: {sbt_script}")

        # Make sbt executable
        os.chmod(sbt_script, 0o755)

        # Build timeout is 2x normal (Delta builds are slow)
        delta_timeout = self.op_config.build_timeout * 2
        self._debug("  delta_timeout=", delta_timeout, "seconds")

        # Use temp Maven repo so the build is reproducible and does not pollute ~/.m2.
        delta_env = self._get_maven_local_env()

        # CrossSparkVersions: runOnlyForReleasableSparkModules only publishes Spark-dependent
        # modules (e.g. delta-spark_4.1). Spark-independent modules (kernel, kernel-unitycatalog,
        # storage) are published by a full publishM2. So we run publishM2 first, then
        # runOnlyForReleasableSparkModules publishM2 for the requested Spark version.
        spark_version = self.delta_config.spark_version
        # Delta's CrossSparkVersions matches by fullVersion (e.g. 4.1.0) or shortVersion (e.g. 4.1).
        # Normalize 4.1.0-SNAPSHOT -> 4.1 so it matches.
        if spark_version and "-SNAPSHOT" in spark_version:
            parts = spark_version.split("-")[0].split(".")
            if len(parts) >= 2:
                spark_version = f"{parts[0]}.{parts[1]}"
        cross_spark = (delta_dir / "project" / "CrossSparkVersions.scala").exists()
        self._debug("  spark_version=", spark_version, "cross_spark (CrossSparkVersions)=", cross_spark)

        try:
            if cross_spark:
                # Publish all modules (kernel, kernel-unitycatalog, storage, etc.) first.
                if self.op_config.verbose:
                    print("[SparkShell] Publishing all Delta modules (kernel, kernel-unitycatalog, storage, ...)...")
                self._run_command(
                    [str(sbt_script), "clean", "publishM2"],
                    cwd=delta_dir,
                    timeout=delta_timeout,
                    check=True,
                    force_output=True,
                    env=delta_env,
                )
                # Then publish Spark-dependent modules for the requested Spark version.
                if self.op_config.verbose:
                    print(f"[SparkShell] Publishing Delta Spark modules for -DsparkVersion={spark_version}...")
                self._run_command(
                    [str(sbt_script), f"-DsparkVersion={spark_version}", "runOnlyForReleasableSparkModules publishM2"],
                    cwd=delta_dir,
                    timeout=delta_timeout,
                    check=True,
                    force_output=True,
                    env=delta_env,
                )
                # CrossSpark publish does not always publish delta-storage; publish it explicitly.
                if self.op_config.verbose:
                    print("[SparkShell] Publishing Delta storage module...")
                self._run_command(
                    [str(sbt_script), "storage/publishM2"],
                    cwd=delta_dir,
                    timeout=delta_timeout,
                    check=True,
                    force_output=True,
                    env=delta_env,
                )
            else:
                self._run_command(
                    [str(sbt_script), f"-DsparkVersion={spark_version}", "clean", "package", "publishM2"],
                    cwd=delta_dir,
                    timeout=delta_timeout,
                    check=True,
                    force_output=True,
                    env=delta_env,
                )
            self._debug("_build_delta: Delta Lake build completed successfully")
            print("[SparkShell] Delta Lake build complete")
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"Delta build timeout after {delta_timeout} seconds\n"
                f"Delta builds can take 10+ minutes. Consider increasing build_timeout in OpConfig."
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Delta Lake build failed\n"
                f"This could be due to:\n"
                f"  1. Build errors in Delta Lake code\n"
                f"  2. Missing dependencies\n"
                f"  3. Incompatible Scala/JVM version\n"
                f"Check the build output above for details."
            )

    def _get_delta_version(self, delta_dir: Path) -> str:
        """
        Extract Delta version from version.sbt or use snapshot.

        Args:
            delta_dir: Path to Delta repository

        Returns:
            Delta version string
        """
        version_file = delta_dir / "version.sbt"
        self._debug("_get_delta_version: version_file=", version_file, "exists=", version_file.exists())

        if version_file.exists():
            content = version_file.read_text()
            # Parse: version := "3.2.0"
            import re
            match = re.search(r'version\s*:=\s*"([^"]+)"', content)
            if match:
                ver = match.group(1)
                self._debug("  parsed version:", ver)
                return ver
            self._debug("  no version match in content, using default")
        else:
            self._debug("  no version.sbt, using default 0.0.0-SNAPSHOT")

        # Default to snapshot version
        return "0.0.0-SNAPSHOT"

    def _setup_uc(self) -> Path:
        """
        Clone Unity Catalog repository and checkout specified branch.

        Returns:
            Path to Unity Catalog directory
        """
        # Local-dev mode: use pre-existing local UC source directly.
        if self.uc_source_config.source_dir:
            local_uc_dir = Path(self.uc_source_config.source_dir).expanduser().resolve()
            self._debug("_setup_uc: source mode=local_dir path=", local_uc_dir)
            if not local_uc_dir.exists():
                raise RuntimeError(
                    f"Configured local Unity Catalog directory does not exist: {local_uc_dir}\n"
                    f"Set UnityCatalogSourceConfig.source_dir to a valid path."
                )
            if not (local_uc_dir / "build.sbt").exists():
                raise RuntimeError(
                    f"Local Unity Catalog directory does not contain build.sbt: {local_uc_dir}"
                )
            print(f"[SparkShell] Unity Catalog source mode: local_dir ({local_uc_dir})")
            return local_uc_dir

        uc_dir = self.work_dir / "unitycatalog"
        self._debug("_setup_uc: uc_dir=", uc_dir, "exists=", uc_dir.exists())
        print("[SparkShell] Unity Catalog source mode: git_clone")

        if uc_dir.exists():
            self._debug("  UC dir already exists, skipping clone")
            if self.op_config.verbose:
                print(f"[SparkShell] Unity Catalog directory already exists: {uc_dir}")
            return uc_dir

        self._debug("  Cloning from", self.uc_source_config.source_repo, "branch", self.uc_source_config.source_branch)
        print(f"[SparkShell] Cloning Unity Catalog from {self.uc_source_config.source_repo}...")

        try:
            self._run_command(
                ["git", "clone", self.uc_source_config.source_repo, "unitycatalog"],
                cwd=self.work_dir,
                timeout=300,
                check=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to clone Unity Catalog repository from {self.uc_source_config.source_repo}\n"
                f"Error: {e}\n"
                f"Please check that the repository URL is correct and accessible."
            )

        print(f"[SparkShell] Checking out branch: {self.uc_source_config.source_branch}")

        try:
            self._run_command(
                ["git", "checkout", self.uc_source_config.source_branch],
                cwd=uc_dir,
                timeout=30,
                check=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to checkout branch '{self.uc_source_config.source_branch}'\n"
                f"Error: {e}\n"
                f"Please verify the branch exists in the repository."
            )

        # Verify build.sbt exists
        if not (uc_dir / "build.sbt").exists():
            raise RuntimeError(
                f"Unity Catalog repository does not contain build.sbt\n"
                f"Directory: {uc_dir}\n"
                f"This may not be a valid Unity Catalog repository."
            )

        print(f"[SparkShell] Unity Catalog repository ready: {uc_dir}")
        return uc_dir

    def _build_uc(self, uc_dir: Path):
        """
        Build Unity Catalog Spark connector and publish to local Maven repository.

        Args:
            uc_dir: Path to Unity Catalog repository
        """
        self._debug("_build_uc: uc_dir=", uc_dir)
        print("[SparkShell] Building Unity Catalog Spark connector from source...")
        print("[SparkShell] This may take a few minutes on first run...")

        sbt_script = uc_dir / "build" / "sbt"
        self._debug("  sbt_script=", sbt_script, "exists=", sbt_script.exists())
        if not sbt_script.exists():
            raise FileNotFoundError(f"Unity Catalog SBT script not found: {sbt_script}")

        # Make sbt executable
        os.chmod(sbt_script, 0o755)

        # UC build timeout
        uc_timeout = self.op_config.build_timeout
        self._debug("  uc_timeout=", uc_timeout, "command: client/publishM2 (skip docs) spark/publishM2 (Maven -> ~/.m2/repository)")

        # Publish to the same temp Maven repo as Delta so SparkShell resolves from one place.
        uc_env = self._get_maven_local_env()
        try:
            # Publish to Maven local so Coursier-based resolution in SparkShell picks up local UC builds.
            # Must publish both client and spark modules since spark depends on client.
            # For client: First compile to trigger OpenAPI code generation, then publish without docs.
            # The Compile/sourceGenerators dependency should trigger OpenAPI generation automatically,
            # but we explicitly run compile first to ensure it happens.
            self._run_command(
                [str(sbt_script), 
                 "client/compile",
                 "set client / packageDoc / publishArtifact := false",
                 "client/publishM2",
                 "spark/publishM2"],
                cwd=uc_dir,
                timeout=uc_timeout,
                check=True,
                force_output=True,
                env=uc_env,
            )
            self._debug("_build_uc: UC build completed successfully")
            print("[SparkShell] Unity Catalog build complete")
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"Unity Catalog build timeout after {uc_timeout} seconds\n"
                f"Consider increasing build_timeout in OpConfig."
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Unity Catalog build failed\n"
                f"This could be due to:\n"
                f"  1. Build errors in Unity Catalog code\n"
                f"  2. Missing dependencies\n"
                f"  3. Incompatible Scala/JVM version (requires Java 17)\n"
                f"Check the build output above for details."
            )

    def _run_command(self, cmd, cwd=None, timeout=None, check=True, force_output=False, env=None):
        """
        Run a command with optional verbose output.

        Args:
            cmd: Command and arguments as list
            cwd: Working directory
            timeout: Timeout in seconds
            check: Raise exception on non-zero exit code
            force_output: If True, stream output even when verbose=False
            env: Environment variables to pass (dict)

        Returns:
            subprocess.CompletedProcess
        """
        self._debug("_run_command: cwd=", cwd, "timeout=", timeout, "check=", check)
        self._debug("  cmd:", " ".join(str(x) for x in cmd))
        if env:
            self._debug("  env (overrides):", list(env.keys()))
            if self.op_config.verbose:
                print(f"[SparkShell] Environment variables: {env}")

        # Build environment
        command_env = os.environ.copy()
        if env:
            command_env.update(env)

        if self.op_config.verbose:
            print(f"[SparkShell] Running: {' '.join(cmd)}")

        if self.op_config.verbose or force_output:
            # Stream output in real-time
            result = subprocess.run(
                cmd,
                cwd=cwd,
                timeout=timeout,
                text=True,
                env=command_env
            )
            if check and result.returncode != 0:
                raise subprocess.CalledProcessError(result.returncode, cmd)
            self._debug("  _run_command completed, returncode=", result.returncode)
            return result
        else:
            # Capture output silently
            result = subprocess.run(
                cmd,
                cwd=cwd,
                timeout=timeout,
                check=check,
                capture_output=True,
                text=True,
                env=command_env
            )
            self._debug("  _run_command completed (captured), returncode=", result.returncode)
            return result

    def __enter__(self):
        """Context manager entry - start server (setup and build happen automatically)."""
        if self.op_config.auto_start:
            self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup."""
        self.shutdown()
        if self.op_config.cleanup_on_exit:
            self.cleanup()
        return False
    
    def _start_output_log(self):
        """Start piping all stdout to a log file in addition to console."""
        if self._original_stdout is not None:
            return  # already teeing
        self._original_stdout = sys.stdout
        tmp_log = Path("/tmp") / f"sparkshell_output_{os.getpid()}.log"
        try:
            f = open(tmp_log, "w")
            tee = _TeeOutput(self._original_stdout, [f])
            self._tee_instance = tee
            sys.stdout = tee
            print(f"[SparkShell] All output is also written to: {tmp_log}")
        except OSError:
            sys.stdout = self._original_stdout
            self._original_stdout = None
            self._tee_instance = None

    def _add_output_log_file(self, path: Path):
        """Add another log file to the tee (e.g. work_dir / sparkshell_output.log). Gets full output from start via buffer."""
        if self._tee_instance is None:
            return
        try:
            f = open(path, "w")
            self._tee_instance.add_file(f)
            print(f"[SparkShell] All output is also written to: {path}")
        except OSError:
            pass

    def _stop_output_log(self):
        """Stop teeing and close log files."""
        if self._original_stdout is None:
            return
        sys.stdout = self._original_stdout
        self._original_stdout = None
        if self._tee_instance is not None:
            for f in self._tee_instance._log_files:
                try:
                    f.close()
                except (OSError, ValueError):
                    pass
            self._tee_instance = None

    def setup(self, force_refresh: bool = False):
        """
        Download or copy SparkApp code to temp directory.

        Args:
            force_refresh: If True, bypass cache and download/copy fresh source
        """
        self._start_output_log()
        self._debug("setup() called, force_refresh=", force_refresh)
        print(f"[SparkShell] Setting up from source: {self.source}")

        # Create temp directory (but we might switch to cache later)
        if self.temp_dir:
            self.work_dir = Path(self.temp_dir)
            self.work_dir.mkdir(parents=True, exist_ok=True)
            self._debug("  using temp_dir from config:", self.work_dir)
        else:
            # If using cache, use cache directory; otherwise use temp
            has_cache = self._has_cached_build()
            self._debug("  has_cached_build:", has_cache)
            if not force_refresh and has_cache:
                self.work_dir = self._get_cache_dir()
                self._debug("  using cache as work_dir:", self.work_dir)
            else:
                self.work_dir = Path(tempfile.mkdtemp(prefix="sparkshell_"))
                self._debug("  created temp work_dir:", self.work_dir)

        print(f"[SparkShell] Working directory: {self.work_dir}")

        # If using cached build, skip download/copy
        if not force_refresh and self._has_cached_build() and self.work_dir == self._get_cache_dir():
            self._debug("  skipping download/copy: using existing cached source")
            print("[SparkShell] Using existing cached source")
        else:
            # Determine if source is GitHub URL or local path
            if self.source.startswith("http://") or self.source.startswith("https://"):
                self._debug("  source is URL -> _download_from_github()")
                self._download_from_github()
            else:
                self._debug("  source is local path -> _copy_from_local()")
                self._copy_from_local()

            # Verify required files exist
            required_files = ["build.sbt", "build/sbt"]
            for file in required_files:
                if not (self.work_dir / file).exists():
                    raise FileNotFoundError(
                        f"Required file not found: {file}. "
                        f"Ensure source contains a valid SparkApp project."
                    )

        # Add work_dir log path so the main log is next to sparkshell.log
        if self.work_dir:
            self._add_output_log_file(self.work_dir / "sparkshell_output.log")
        self._debug("setup() complete. work_dir=", self.work_dir)
        print("[SparkShell] Setup complete")
    
    def _download_from_github(self):
        """Download SparkApp code from GitHub."""
        print("[SparkShell] Downloading from GitHub...")
        
        # Parse GitHub URL to get repo and path
        # Support formats:
        # - https://github.com/user/repo/tree/branch/path/to/dir
        # - https://github.com/user/repo (clone entire repo)
        
        if "/tree/" in self.source:
            # Sparse checkout for specific directory
            parts = self.source.split("/tree/")
            repo_url = parts[0]
            branch_and_path = parts[1].split("/", 1)
            branch = branch_and_path[0]
            subdir = branch_and_path[1] if len(branch_and_path) > 1 else ""
            
            # Clone with sparse checkout
            try:
                # Initialize git repo
                self._run_command(["git", "init"], cwd=self.work_dir)

                # Add remote
                self._run_command(["git", "remote", "add", "origin", repo_url], cwd=self.work_dir)

                # Enable sparse checkout
                self._run_command(["git", "config", "core.sparseCheckout", "true"], cwd=self.work_dir)

                # Specify path to checkout
                sparse_checkout_file = self.work_dir / ".git" / "info" / "sparse-checkout"
                sparse_checkout_file.write_text(f"{subdir}\n")

                # Pull the specific branch
                self._run_command(["git", "pull", "origin", branch, "--depth=1"], cwd=self.work_dir)
                
                # Move files from subdir to root if needed
                if subdir:
                    subdir_path = self.work_dir / subdir
                    if subdir_path.exists():
                        for item in subdir_path.iterdir():
                            shutil.move(str(item), str(self.work_dir / item.name))
                        # Remove empty subdirectories
                        shutil.rmtree(subdir_path.parent if subdir_path.parent != self.work_dir else subdir_path)

                print("[SparkShell] Download complete")
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"Failed to clone from GitHub: {e.stderr.decode() if e.stderr else str(e)}")
        else:
            # Full repo clone
            try:
                self._run_command(["git", "clone", "--depth=1", self.source, str(self.work_dir)])
                print("[SparkShell] Clone complete")
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"Failed to clone from GitHub: {str(e)}")
    
    def _copy_from_local(self):
        """Copy SparkApp code from local directory."""
        print("[SparkShell] Copying from local directory...")

        source_path = Path(self.source).expanduser().resolve()
        if not source_path.exists():
            raise FileNotFoundError(f"Source directory not found: {source_path}")

        # Copy all files
        for item in source_path.iterdir():
            if item.name in [".git", "target", "project/target", "sparkapp.log", "sparkapp.pid"]:
                continue  # Skip unnecessary files

            dest = self.work_dir / item.name
            if item.is_dir():
                shutil.copytree(item, dest, ignore=shutil.ignore_patterns("target", ".git"))
            else:
                shutil.copy2(item, dest)

        print("[SparkShell] Copy complete")
    
    def build(self, force_refresh: bool = False):
        """
        Build the assembly JAR using SBT.

        Args:
            force_refresh: If True, force rebuild even if cached build exists
        """
        self._debug("build() called, force_refresh=", force_refresh)
        # Check if we can use cached build
        has_cache = self._has_cached_build()
        self._debug("  has_cached_build:", has_cache)
        if self.op_config.verbose:
            print(f"[SparkShell] Build decision:")
            print(f"  Force refresh: {force_refresh}")

        if not force_refresh and has_cache:
            self._debug("  decision: using cached build")
            if self.op_config.verbose:
                print(f"[SparkShell] Decision: Using cached build (cache exists and no force refresh)")
            self._use_cached_build()
            # Ensure .sbtopts is present in the cached work_dir
            self._ensure_sbtopts()
            self._debug("build() complete (cache). jar_path=", self.jar_path)
            print("[SparkShell] Build complete (using cache)")
            return
        else:
            self._debug("  decision: building from scratch (force_refresh=", force_refresh, "or no cache)")
        if self.op_config.verbose:
            if force_refresh:
                print(f"[SparkShell] Decision: Building from scratch (force refresh requested)")
            else:
                print(f"[SparkShell] Decision: Building from scratch (no cache available)")

        # Ensure .sbtopts is present in work_dir before building
        self._ensure_sbtopts()

        # Use a temp Maven repo under work_dir so the build is reproducible and does not pollute ~/.m2.
        self._m2_repo = self.work_dir / "m2_repo"
        self._m2_repo.mkdir(parents=True, exist_ok=True)
        if self.op_config.verbose:
            print(f"[SparkShell] Maven local repo (this build only): {self._m2_repo.resolve()}")

        # Patch SparkShell's own build/sbt for the temp repo
        self._patch_sbt_for_temp_repo(self.work_dir)

        # Setup and build Delta from source
        if self.op_config.verbose:
            print(f"[SparkShell] Setting up Delta Lake:")
            if self.delta_config.source_dir:
                print(f"  Source mode: local_dir")
                print(f"  Path: {self.delta_config.source_dir}")
            else:
                print(f"  Source mode: git_clone")
                print(f"  Repository: {self.delta_config.source_repo}")
                print(f"  Branch: {self.delta_config.source_branch}")

        delta_dir = self._setup_delta()
        self._patch_sbt_for_temp_repo(delta_dir)
        self._build_delta(delta_dir)
        delta_version = self._get_delta_version(delta_dir)
        self._debug("build: delta_version for SparkShell sbt env:", delta_version)

        # Setup and build Unity Catalog from source (for FGAC support)
        if self.op_config.verbose:
            print(f"[SparkShell] Setting up Unity Catalog:")
            if self.uc_source_config.source_dir:
                print(f"  Source mode: local_dir")
                print(f"  Path: {self.uc_source_config.source_dir}")
            else:
                print(f"  Source mode: git_clone")
                print(f"  Repository: {self.uc_source_config.source_repo}")
                print(f"  Branch: {self.uc_source_config.source_branch}")

        uc_dir = self._setup_uc()
        self._patch_sbt_for_temp_repo(uc_dir)
        self._build_uc(uc_dir)

        # Always print version information (not just in verbose mode)
        print(f"[SparkShell] ========================================")
        print(f"[SparkShell] Build Configuration:")
        print(f"[SparkShell]   Scala:  {self.build_config.scala_version}")
        print(f"[SparkShell]   Spark:  {self.delta_config.spark_dep_version}")
        if self.delta_config.source_dir:
            print(f"[SparkShell]   Delta:  {delta_version} (source mode: local_dir)")
            print(f"[SparkShell]           path: {Path(self.delta_config.source_dir).expanduser().resolve()}")
        else:
            print(f"[SparkShell]   Delta:  {delta_version} (built from {self.delta_config.source_branch})")
        if self.uc_source_config.source_dir:
            print(f"[SparkShell]   UC:     0.5.0-SNAPSHOT (source mode: local_dir)")
            print(f"[SparkShell]           path: {Path(self.uc_source_config.source_dir).expanduser().resolve()}")
        else:
            print(f"[SparkShell]   UC:     0.5.0-SNAPSHOT or 0.3.1 (built from {self.uc_source_config.source_branch}; UC_USE_LOCAL selects version)")
        print(f"[SparkShell]   ANTLR:  4.13.1 (via Spark Master)")
        print(f"[SparkShell] ========================================")

        print("[SparkShell] Building SparkShell assembly JAR...")
        print("[SparkShell] This may take several minutes on first run...")

        sbt_script = self.work_dir / "build" / "sbt"
        if not sbt_script.exists():
            raise FileNotFoundError(f"SBT script not found: {sbt_script}")

        # Make sbt executable
        os.chmod(sbt_script, 0o755)

        # Derive Hadoop version from Spark version if not explicitly set.
        # Spark 4.1.x ships with Hadoop 3.4.1; Spark 4.0.x with 3.4.0.
        hadoop_version = self.build_config.hadoop_version
        if not hadoop_version:
            spark_dep = self.delta_config.spark_dep_version
            if spark_dep.startswith("4.1"):
                hadoop_version = "3.4.1"
            else:
                hadoop_version = "3.4.0"

        # Create environment variables for SBT (use same temp Maven repo for resolution).
        build_env = {
            "DELTA_VERSION": delta_version,
            "DELTA_SPARK_VERSION": self.delta_config.spark_version,
            "SPARK_VERSION": self.delta_config.spark_dep_version,
            "SCALA_VERSION": self.build_config.scala_version,
            "HADOOP_VERSION": hadoop_version,
            "DELTA_USE_LOCAL": "true",
            "UC_USE_LOCAL": "true"
        }
        build_env.update(self._get_maven_local_env())
        self._debug(
            "build: SparkShell sbt env: DELTA_VERSION=",
            delta_version,
            "DELTA_SPARK_VERSION=",
            self.delta_config.spark_version,
            "SPARK_VERSION=",
            self.delta_config.spark_dep_version,
            "SCALA_VERSION=",
            self.build_config.scala_version,
            "DELTA_USE_LOCAL=true UC_USE_LOCAL=true")
        self._debug("build: running sbt assembly from work_dir=", self.work_dir)

        try:
            # Run sbt assembly - always show output so users see build progress
            result = self._run_command(
                [str(sbt_script), "assembly"],
                cwd=self.work_dir,
                timeout=self.op_config.build_timeout,
                check=True,
                force_output=True,
                env=build_env
            )

            # Find the JAR file
            jar_path = self.work_dir / "target" / "scala-2.13" / "sparkshell.jar"
            self._debug("build: expected jar_path=", jar_path, "exists=", jar_path.exists())
            if not jar_path.exists():
                raise FileNotFoundError(f"Assembly JAR not found at: {jar_path}")

            self.jar_path = jar_path
            self._debug("build() complete. jar_path=", self.jar_path)
            print(f"[SparkShell] Build complete: {self.jar_path}")

            # Cache the build for future reuse
            self._cache_build()

        except subprocess.TimeoutExpired:
            raise RuntimeError(f"Build timeout after {self.op_config.build_timeout} seconds")
        except subprocess.CalledProcessError as e:
            error_msg = str(e)
            if "delta-spark" in error_msg or "io.delta" in error_msg:
                raise RuntimeError(
                    f"SparkShell build failed: Could not resolve Delta Lake dependency\n"
                    f"Delta version: {delta_version}\n"
                    f"This could mean:\n"
                    f"  1. Delta publishLocal did not complete successfully\n"
                    f"  2. Maven local repository is corrupted\n"
                    f"  3. Incompatible Delta version specified\n"
                    f"Try: rm -rf ~/.m2/repository/io/delta and rebuild with force_refresh=True"
                )
            else:
                raise RuntimeError(f"SparkShell build failed: {error_msg}")
    
    def start(self, force_refresh: bool = False):
        """
        Start the SparkApp server (automatically handles setup and build if needed).

        Args:
            force_refresh: If True, force fresh download and rebuild, bypassing cache (default: False)
        """
        self._debug("start() called, force_refresh=", force_refresh)
        # Automatically setup if not already done
        if not self.work_dir:
            self._debug("  work_dir not set -> calling setup()")
            self.setup(force_refresh=force_refresh)
        else:
            self._debug("  work_dir already set:", self.work_dir)

        # Automatically build if not already done
        if not self.jar_path or not self.jar_path.exists():
            self._debug("  jar_path missing or invalid -> calling build()")
            self.build(force_refresh=force_refresh)
        else:
            self._debug("  jar_path already set:", self.jar_path)

        self._debug("start: port=", self.port, "base_url=", self.base_url)
        print(f"[SparkShell] Starting server on port {self.port}...")
        fgac_log("SparkShell.start", f"Starting server on port {self.port}")
        fgac_log("SparkShell.start", f"JAR path: {self.jar_path}")
        fgac_log("SparkShell.start", f"Work dir: {self.work_dir}")

        # Check if port is already in use
        port_in_use = self._is_port_in_use()
        self._debug("  port", self.port, "in use:", port_in_use)
        if port_in_use:
            raise RuntimeError(f"Port {self.port} is already in use")
        
        # Start the server process
        log_file = self.work_dir / "sparkshell.log"
        self._debug("  log_file=", log_file)

        # Build command with port and optional Spark configs
        server_env = self._get_maven_local_env()
        java_home = server_env.get("JAVA_HOME", os.environ.get("JAVA_HOME", ""))
        java_cmd = os.path.join(java_home, "bin", "java") if java_home else "java"
        cmd = [java_cmd, "-jar", str(self.jar_path), str(self.port)]
        self._debug("  JAVA_HOME=", java_home, "java_cmd=", java_cmd)
        self._debug("  full start cmd:", " ".join(cmd))

        # Add Spark configurations as key=value arguments
        if self.spark_config.configs:
            for key, value in self.spark_config.configs.items():
                cmd.append(f"{key}={value}")
                print(f"[SparkShell] Setting Spark config: {key}={value}")

        if self.op_config.verbose:
            print(f"[SparkShell] Running: {' '.join(cmd)}")

        # Keep the log file open for the lifetime of the server process so
        # output is always captured.  A background thread drains stdout so
        # the OS pipe buffer never fills up (which would block the JVM).
        import threading

        self._log_handle = open(log_file, "w")

        self.process = subprocess.Popen(
            cmd,
            cwd=self.work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            preexec_fn=os.setsid if sys.platform != "win32" else None,
        )

        def _drain_stdout():
            """Continuously read server stdout, write to log file (and print in verbose mode)."""
            try:
                for line in self.process.stdout:
                    self._log_handle.write(line)
                    self._log_handle.flush()
                    if self.op_config.verbose:
                        print(line, end='')
            except (ValueError, OSError):
                pass
            finally:
                try:
                    self._log_handle.close()
                except (ValueError, OSError):
                    pass

        self._log_thread = threading.Thread(target=_drain_stdout, daemon=True)
        self._log_thread.start()

        # Wait for server to be ready
        self._debug("start: waiting up to", self.op_config.startup_timeout, "s for server")
        print("[SparkShell] Waiting for server to start...")
        start_time = time.time()

        while time.time() - start_time < self.op_config.startup_timeout:
            if self._check_health():
                self.is_ready = True
                self._debug("start: server health check passed, is_ready=True")
                print(f"[SparkShell] Server ready at {self.base_url}")

                # Set Unity Catalog schema if configured (catalog is already set via defaultCatalog config)
                if self.uc_config.uri and self.uc_config.token:
                    print(f"[SparkShell] Unity Catalog enabled: {self.uc_config.catalog}")

                    if self.uc_config.schema:
                        try:
                            print(f"[SparkShell] Setting default schema: {self.uc_config.schema}")
                            self.execute_sql(f"USE {self.uc_config.schema}")
                            print(f"[SparkShell] Tables can be referenced as: {self.uc_config.catalog}.{self.uc_config.schema}.table_name or table_name")
                        except RuntimeError as e:
                            print(f"[SparkShell] Warning: Failed to set schema: {e}")
                            print(f"[SparkShell] Tables can be referenced as: {self.uc_config.catalog}.{self.uc_config.schema}.table_name")
                    else:
                        print(f"[SparkShell] Tables must be referenced as: {self.uc_config.catalog}.schema.table_name")

                return

            # Check if process died
            if self.process.poll() is not None:
                with open(log_file) as f:
                    log_contents = f.read()
                raise RuntimeError(f"Server process died. Log:\n{log_contents}")

            time.sleep(1)

        raise RuntimeError(f"Server failed to start within {self.op_config.startup_timeout} seconds")
    
    def _is_port_in_use(self) -> bool:
        """Check if the port is already in use."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=2)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def _check_health(self) -> bool:
        """Check if server is healthy."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=2)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def execute_sql(self, sql: str, output_path: Optional[str] = None) -> str:
        """
        Execute SQL command and return result output or write to Parquet.

        Args:
            sql: SQL command to execute
            output_path: Optional path to write results as Parquet files.
                        If provided, results are written to disk instead of returned.
                        Supports local paths (/tmp/output) and cloud paths (s3://bucket/path, dbfs:/path)

        Returns:
            str: Query result as formatted string, OR write metadata if output_path provided

        Raises:
            RuntimeError: If server is not ready or SQL execution fails

        Examples:
            # Return results as string (default)
            result = shell.execute_sql("SELECT * FROM table LIMIT 10")
            
            # Write results to Parquet
            metadata = shell.execute_sql("SELECT * FROM large_table", output_path="/tmp/results")
        """
        if not self.is_ready:
            raise RuntimeError("Server is not ready. Call start() first.")

        fgac_log("SparkShell.execute_sql", f"Executing SQL: {sql[:100]}...")
        if output_path:
            fgac_log("SparkShell.execute_sql", f"Output path: {output_path}")

        try:
            # Build request payload
            payload = {"sql": sql}
            if output_path:
                payload["outputPath"] = output_path
            
            response = requests.post(
                f"{self.base_url}/sql",
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=300  # 5 minutes timeout for long queries
            )
            fgac_log("SparkShell.execute_sql", f"Response status: {response.status_code}")

            if response.status_code != 200:
                self._print_spark_logs_on_error()
                raise RuntimeError(f"HTTP error {response.status_code}: {response.text}")

            data = response.json()

            if not data.get("success", False):
                error_msg = data.get("error", "Unknown error")
                self._print_spark_logs_on_error()
                raise RuntimeError(f"SQL execution failed: {error_msg}")

            return data.get("result", "")

        except requests.exceptions.RequestException as e:
            self._print_spark_logs_on_error()
            raise RuntimeError(f"Failed to execute SQL: {str(e)}")

    def _print_spark_logs_on_error(self, num_lines: int = 50):
        """
        Print the last N lines from the Spark log file when an error occurs.

        Args:
            num_lines: Number of lines to show from the end of the log file
        """
        if not self.work_dir:
            return

        log_file = self.work_dir / "sparkshell.log"

        if not log_file.exists():
            print("[SparkShell] Log file not found")
            return

        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()

            # Get the last N lines
            tail_lines = lines[-num_lines:] if len(lines) > num_lines else lines

            print("\n" + "="*70)
            print(f"[SparkShell] Last {len(tail_lines)} lines from Spark logs:")
            print("="*70)
            for line in tail_lines:
                print(line, end='')
            print("="*70 + "\n")
        except Exception as e:
            print(f"[SparkShell] Failed to read log file: {e}")
    
    def get_server_info(self) -> dict:
        """Get server information."""
        if not self.is_ready:
            raise RuntimeError("Server is not ready. Call start() first.")
        
        try:
            response = requests.get(f"{self.base_url}/info", timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to get server info: {str(e)}")
    
    def shutdown(self):
        """Shutdown the server gracefully."""
        self._debug("shutdown() called, process is", "None" if self.process is None else "running")
        if self.process is None:
            return
        
        print("[SparkShell] Shutting down server...")
        
        try:
            # Try graceful shutdown first
            self.process.terminate()
            
            # Wait up to 10 seconds for graceful shutdown
            try:
                self.process.wait(timeout=10)
                print("[SparkShell] Server shutdown complete")
            except subprocess.TimeoutExpired:
                print("[SparkShell] Forcing server shutdown...")
                self.process.kill()
                self.process.wait()
                print("[SparkShell] Server killed")
        except Exception as e:
            print(f"[SparkShell] Error during shutdown: {e}")
        finally:
            if hasattr(self, '_log_thread') and self._log_thread is not None:
                self._log_thread.join(timeout=5)
                self._log_thread = None
            if hasattr(self, '_log_handle') and self._log_handle is not None:
                try:
                    self._log_handle.close()
                except (ValueError, OSError):
                    pass
                self._log_handle = None
            self.process = None
            self.is_ready = False
    
    def cleanup(self):
        """Clean up temporary files (but never delete the cache)."""
        self._stop_output_log()
        self._debug("cleanup() called, work_dir=", self.work_dir)
        if not self.work_dir or not self.work_dir.exists():
            self._debug("  skip: no work_dir or does not exist")
            return

        cache_dir = self._get_cache_dir()

        # Never delete the cache directory
        if self.work_dir == cache_dir:
            self._debug("  skip: work_dir is cache dir (never delete cache)")
            if self.op_config.verbose:
                print(f"[SparkShell] Skipping cleanup: work_dir is cache directory")
            return

        self._debug("  removing work_dir (temp):", self.work_dir)
        print(f"[SparkShell] Cleaning up: {self.work_dir}")
        try:
            shutil.rmtree(self.work_dir)
            print("[SparkShell] Cleanup complete")
        except Exception as e:
            print(f"[SparkShell] Error during cleanup: {e}")
    
    def __del__(self):
        """Destructor - ensure cleanup."""
        if hasattr(self, 'process') and self.process:
            self.shutdown()


# For usage examples, see spark_shell_example.py

