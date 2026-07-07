# Examples CI/CD Workflow Documentation

This document describes the dedicated GitHub Actions workflow for validating StarRocks Stream Load examples.
This workflow ensures all example programs compile successfully, follow code quality standards, and maintain consistency.

## 📊 Workflow Overview

- **File**: `.github/workflows/examples-ci.yml`
- **Platform**: arc-runner-set
- **Scope**: Complete validation of 12 production-grade examples (5,465+ lines of code)
- **Frequency**: On examples changes, PRs, weekly maintenance, and manual trigger

## 🎯 Purpose

The Examples CI workflow provides:
- ✅ **Compilation Validation**: Ensures all examples compile without errors
- ✅ **Code Quality**: Lint checks via clippy with strict warnings
- ✅ **Formatting**: Consistent code style via rustfmt
- ✅ **Documentation**: Builds documentation without warnings
- ✅ **Library Testing**: Validates shared utilities work correctly
- ✅ **Structure Validation**: Ensures example files and configuration are complete

## 🔄 Trigger Conditions

### Automatic Triggers
1. **Push to main branch**: Validates examples stay current
2. **Pull requests with examples changes**: Focused validation
3. **Weekly schedule**: Maintenance check (Mondays 2am UTC)
4. **Examples directory changes**: Path-specific triggering

### Manual Trigger
```bash
gh workflow run examples-ci.yml
```

## 🏗️ Workflow Architecture

### Phase 1: Foundation Checks (Parallel Execution)
```
examples-compile  ─┐
examples-format  ─┼→ Fast path gates (run in parallel)
                  ┘
```

### Phase 2: Quality Gates (Parallel Execution)
```
examples-lint    ─┐
examples-doc     ─┼→ Depends on Phase 1 + examples-compile
examples-lib-test ─┘
```

### Phase 3: Validation & Reporting
```
examples-validate → Comprehensive status summary and reporting
```

## 📋 Job Descriptions

### examples-compile
**Purpose**: Validates all example binaries compile successfully
**Steps**:
1. Checkout repository
2. Check if examples changed (path filtering)
3. Install dependencies and Rust toolchain
4. Cache Cargo artifacts for speed
5. Build shared library
6. Build all 12 example binaries
7. Validate compilation completeness

**Output**: Compiled examples list and validation status

### examples-format
**Purpose**: Ensures all example code follows rustfmt standards
**Steps**:
1. Checkout repository
2. Install Rust toolchain with rustfmt
3. Check formatting compliance
4. Validate code style consistency

**Output**: Formatting validation status

### examples-lint
**Purpose**: Runs clippy linting with strict warnings as errors
**Steps**:
1. Checkout repository
2. Install dependencies and Rust toolchain with clippy
3. Cache Cargo artifacts
4. Run clippy with `-D warnings`
5. Validate code quality passes strict standards

**Output**: Linting results and warning count

### examples-doc
**Purpose**: Builds documentation and validates completeness
**Steps**:
1. Checkout repository
2. Install dependencies and Rust toolchain
3. Cache Cargo artifacts
4. Build documentation with strict warning flags
5. Validate documentation builds without warnings

**Output**: Documentation validation status

### examples-lib-test
**Purpose**: Validates shared utilities and common functionality
**Steps**:
1. Checkout repository
2. Install dependencies and Rust toolchain
3. Cache Cargo artifacts
4. Run library tests
5. Validate shared utilities work correctly

**Output**: Test results and status

### examples-validate
**Purpose**: Final validation and comprehensive reporting
**Steps**:
1. Checkout repository
2. Validate example file structure (all 12 examples present)
3. Validate README documentation completeness
4. Validate Cargo.toml configuration correctness
5. Generate failure report if needed
6. Create comprehensive status summary
7. Add PR comments for failures (if applicable)
8. Provide final success message

**Output**: Overall validation status and detailed report

## 🚀 Examples Validated

### Basic Examples (3)
- `v1_direct_load` - Simple single-shot data loading
- `v2_transaction_basic` - Two-phase commit transactions
- `data_formats` - Multiple data format support

### Production Examples (4)
- `exponential_backoff` - Retry strategy with exponential backoff
- `circuit_breaker` - Circuit breaker for failure prevention
- `metrics_monitoring` - Comprehensive metrics collection
- `transaction_state` - Transaction state management

### Advanced Examples (4)
- `multi_table_transaction` - Cross-table atomic operations
- `error_handling_recovery` - Advanced error handling
- `high_availability` - High availability patterns
- `data_pipeline` - Complete data pipeline integration

### Integration Examples (1)
- `concurrent_loads` - High-throughput concurrent operations

## 🔧 Configuration Options

### Environment Variables
```yaml
CARGO_TERM_COLOR: always
RUSTDOCFLAGS: "-D warnings"
SKIP_EXAMPLES_LINT: "false"
SKIP_EXAMPLES_FORMAT: "false" 
SKIP_EXAMPLES_TEST: "false"
```

### Skip Specific Checks
If needed, you can temporarily skip certain validations:
```bash
# In workflow dispatch or UI
SKIP_EXAMPLES_LINT: "true"  # Skip linting
SKIP_EXAMPLES_FORMAT: "true" # Skip formatting check
SKIP_EXAMPLES_TEST: "true"   # Skip library tests
```

## 📊 Success Criteria

### Must Pass Checks
- ✅ All 12 example binaries compile without errors
- ✅ Examples library passes clippy with `-- -D warnings`
- ✅ All examples follow rustfmt standards
- ✅ Documentation builds without warnings
- ✅ Shared library tests pass
- ✅ Example structure validation passes
- ✅ README.md exists and is complete
- ✅ Cargo.toml configuration is correct

### Performance Targets
- 📈 Compilation time < 2 minutes (with caching)
- 📈 Lint check time < 1 minute (with caching)
- 📈 Total workflow time < 5 minutes (with caching)
- 📈 Cache hit rate > 80%
- 📈 Zero clippy warnings
- 📈 Zero formatting issues

## 🚨 Troubleshooting

### Compilation Failures

**Symptom**: `examples-compile` job fails

**Common Causes**:
- Syntax errors in example code
- Missing dependencies
- Incorrect imports
- Type mismatches

**Solutions**:
1. Check compilation logs for specific errors
2. Verify all dependencies are listed in examples/Cargo.toml
3. Ensure imports are correct and complete
4. Run locally: `cargo build --package starrocks-examples --bin`

### Lint Failures

**Symptom**: `examples-lint` job fails with clippy warnings

**Common Causes**:
- Code style violations
- Potential bugs flagged by clippy
- Deprecated patterns

**Solutions**:
1. Review clippy warnings in the logs
2. Fix warning issues in example code
3. Run locally: `cargo clippy --package starrocks-examples --all-targets -- -D warnings`
4. Use `cargo clippy --fix` for automatic fixes where possible

### Format Failures

**Symptom**: `examples-format` job fails

**Common Causes**:
- Inconsistent code formatting
- Manual formatting that doesn't match rustfmt

**Solutions**:
1. Run locally: `cargo fmt --manifest-path examples/Cargo.toml`
2. Commit the formatted changes
3. Re-run the workflow

### Documentation Failures

**Symptom**: `examples-doc` job fails with warnings

**Common Causes**:
- Missing documentation
- Broken links in docs
- Deprecated APIs

**Solutions**:
1. Check specific doc warnings in logs
2. Add or fix documentation comments
3. Run locally: `cargo doc --package starrocks-examples --no-deps`

## 🎯 Best Practices

### For Developers
- Run local checks before pushing:
  ```bash
  cargo fmt --manifest-path examples/Cargo.toml -- --check
  cargo clippy --package starrocks-examples --all-targets -- -D warnings
  cargo build --package starrocks-examples --bins
  ```

- Follow the learning path in examples/README.md
- Use consistent code style with main SDK
- Ensure documentation is comprehensive
- Test examples locally with mock servers

### For Maintainers
- Keep dependencies updated (Dependabot handles this)
- Monitor workflow performance metrics
- Review and address false positives
- Keep documentation current with example changes
- Periodically review and improve validation logic

## 📈 Monitoring & Metrics

### Key Performance Indicators
- Workflow execution time (target: < 5 minutes)
- Cache hit rates (target: > 80%)
- Compilation success rates (target: 100%)
- Lint warning trends (target: 0)
- PR validation frequency
- Manual trigger usage patterns

### Success Metrics
- ✅ Workflow runs successfully 100% of time
- ✅ All 12 examples compile and validate consistently
- ✅ Zero false-positive failures
- ✅ Execution time remains under 5 minutes
- ✅ Cache hit rates > 80%

## 🔗 Integration Points

### Main CI Workflow
The examples workflow runs independently but complements the main CI workflow (`ci.yml`). Both can run in parallel for faster feedback.

### Release Workflow
Examples are validated as part of the release process in `release.yml` to ensure released versions include working examples.

### Dependabot
Examples dependencies are automatically tracked by Dependabot and updated weekly to stay current with security patches and bug fixes.

## 🚀 Quick Start

### For New Examples

1. Create your example in the appropriate directory:
   ```bash
   examples/basic/my_new_example.rs
   examples/production/my_production_example.rs
   examples/advanced/my_advanced_example.rs
   ```

2. Add binary configuration to `examples/Cargo.toml`:
   ```toml
   [[bin]]
   name = "my_new_example"
   path = "basic/my_new_example.rs"
   ```

3. Update `examples/README.md` with documentation

4. Tests will automatically validate your new example

### For Existing Examples

1. Make your changes
2. Run local validation:
   ```bash
   cargo fmt --manifest-path examples/Cargo.toml
   cargo clippy --package starrocks-examples --all-targets -- -D warnings
   cargo build --package starrocks-examples --bins
   ```
3. Commit and push - the CI will automatically validate

## 📞 Support

### Getting Help
- Check workflow run logs for detailed error messages
- Review this documentation for common issues
- Open a GitHub issue for persistent problems
- Check examples/README.md for example-specific guidance

### Reporting Issues
When reporting workflow failures, include:
- Workflow run link
- Specific job that failed
- Error messages from logs
- Steps to reproduce
- Your environment details (if local testing was done)

## 🔮 Future Enhancements

Planned improvements to the examples CI workflow:
- 🔄 Example execution testing with mock StarRocks servers
- 🔄 Performance benchmarking for production examples
- 🔄 Cross-platform validation (Linux, macOS, Windows)
- 🔄 Example complexity categorization metrics
- 🔄 Automated documentation updates
- 🔄 Integration with CI/CD dashboards

---

**Last Updated**: 2024-07-07  
**Maintained by**: StarRocks Stream Load SDK Team  
**Questions?**: Open an issue or check the main repository documentation