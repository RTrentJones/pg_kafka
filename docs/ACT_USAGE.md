# Using Act for Local CI Testing

This guide shows how to run GitHub Actions workflows locally using [act](https://github.com/nektos/act).

## Quick Start

```bash
# Run the default test job
./bin/test-ci-locally

# Run with verbose output
act -j test --verbose

# List all available jobs
act -j test -l
```

## Accessing Logs

### 1. Standard Output (Easiest)
Act shows workflow output in the terminal by default. Use verbose mode for more details:

```bash
act -j test --verbose
```

### 2. Container Logs (Real-time)

In a **separate terminal**:

```bash
# Watch for new containers
watch -n 1 'docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Status}}"'

# Follow logs from the running container
docker logs -f $(docker ps --filter "name=act-test" --format "{{.ID}}" | head -1)
```

### 3. Postgres/pg_kafka Logs

Our CI workflow now dumps logs automatically, but you can also access them manually:

```bash
# Keep container running after failure
act -j test --rm=false

# List all containers (including stopped ones)
docker ps -a | grep act

# Access the container interactively
docker exec -it <container-id> bash

# Inside the container, find and read logs
find ~/.pgrx -name "logfile" -o -name "*.log"
tail -100 ~/.pgrx/data-14/logfile
```

### 4. Extract Logs from Stopped Container

```bash
# Copy logfile from stopped container
docker cp <container-id>:/home/runner/.pgrx/data-14/logfile ./pg_kafka.log

# View the extracted log
cat pg_kafka.log
```

## Advanced Usage

### Use Official GitHub Runner Images

For better compatibility with actual GitHub Actions:

```bash
# Use catthehacker's images (more complete)
act -j test -P ubuntu-latest=catthehacker/ubuntu:act-latest

# Use nektos images (smaller, faster)
act -j test -P ubuntu-latest=nektos/act-environments-ubuntu:18.04
```

### Debug Specific Steps

```bash
# Run only up to a specific step
act -j test --action <step-number>

# Skip certain steps by modifying workflow temporarily
# (add `if: false` to steps you want to skip)
```

### Environment Variables

```bash
# Set custom environment variables
act -j test --env RUST_LOG=debug --env RUST_BACKTRACE=1

# Use a .env file
act -j test --env-file .env.local
```

### Secrets

```bash
# Pass secrets (though most won't be needed locally)
act -j test --secret CODECOV_TOKEN=fake-token
```

## Debugging Tips

### 1. Keep Container Running on Failure

```bash
act -j test --rm=false --no-cleanup
```

Then you can:
- Inspect the filesystem
- Read logs
- Run commands manually
- Debug why tests failed

### 2. Use Docker Debugging

```bash
# Watch container creation/destruction
docker events --filter 'type=container'

# See what act is running
docker inspect <container-id>

# Check resource usage
docker stats <container-id>
```

### 3. Compare with Actual CI

Run the same workflow both locally and in GitHub Actions, then compare:
- Output logs
- Timing differences
- Environment variables (`env` command output)
- Installed packages

## Known Limitations

### What Works Well
✅ Basic workflow syntax
✅ Environment variables
✅ Multiple jobs
✅ Docker actions
✅ Bash scripts
✅ Service containers

### What Has Issues
⚠️ Caching (works differently than GitHub)
⚠️ Artifacts (upload/download may fail)
⚠️ GitHub-specific contexts (some variables unavailable)
⚠️ Matrix builds (can be slow)
⚠️ Resource limits (depends on Docker configuration)

### Act vs GitHub Actions Differences

| Feature | GitHub Actions | Act |
|---------|---------------|-----|
| Runner | Ubuntu VM | Docker Container |
| Resources | 2 CPU, 7GB RAM | Your Docker limits |
| Networking | Cloud network | localhost |
| Caching | GitHub's cache | Docker volumes |
| Secrets | Encrypted vault | Plain text/env |

## Troubleshooting

### "Cannot connect to Docker daemon"
```bash
# Start Docker
sudo systemctl start docker

# Or on macOS
open -a Docker
```

### "Platform not supported"
```bash
# Force platform
act -j test --container-architecture linux/amd64
```

### "Out of disk space"
```bash
# Clean up old containers and images
docker system prune -a

# See disk usage
docker system df
```

### "Step hangs indefinitely"
Some steps may behave differently in Docker. Try:
- Adding timeouts to long-running commands
- Using `--verbose` to see what's happening
- Checking Docker resource limits

## Performance Tips

1. **Use smaller images**: `nektos/act-environments-ubuntu:18.04` vs `catthehacker/ubuntu:act-latest`
2. **Cache dependencies**: Docker layer caching helps with rebuilds
3. **Limit jobs**: Run only the job you're debugging
4. **Increase Docker resources**: More CPU/RAM = faster builds

## Examples

### Run with full logging
```bash
act -j test --verbose 2>&1 | tee act-run.log
```

### Debug network issues
```bash
# Run and keep container
act -j test --rm=false

# In another terminal
docker exec -it <container-id> bash -c "netstat -tlnp"
```

### Test a specific commit
```bash
git checkout <commit-sha>
act -j test
git checkout -
```

## Resources

- [Act Documentation](https://github.com/nektos/act)
- [Act Runner Images](https://github.com/nektos/act/blob/master/IMAGES.md)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
