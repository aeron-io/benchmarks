# Releasing

Only `benchmarks-api` (`io.aeron.benchmarks:benchmarks-api`) is published, with its sources and Javadoc jars, to
Adaptive's Artifactory. The steps follow Aeron's release. Snapshots are not published.

## Release

1. Set `version.txt` to the release version, e.g. `0.1.0`, and commit it as `0.1.0 released.`
2. Tag that commit with the bare version and push the commit and the tag:

   ```bash
   git tag 0.1.0
   git push origin master 0.1.0
   ```

   The tag starts [`release.yml`](.github/workflows/release.yml). It checks that the tag matches `version.txt` and is
   not a `-SNAPSHOT`, builds `benchmarks-api`, and publishes it to Artifactory.
3. Set `version.txt` to the next snapshot, e.g. `0.2.0-SNAPSHOT`, and commit it as `post release bump`.

## Secrets

| Secret | Use |
|---|---|
| `ARTIFACTORY_PUBLISH_URL` | URL of the Artifactory Maven repository to deploy releases to |
| `ARTIFACTORY_WRITER_ACCESS_TOKEN` | Artifactory access token with deploy rights on that repository |

The build adds the `artifactory` repository only for a release version with both values set, so a snapshot, a local
build or a fork has nowhere to publish to.

## Checking a release locally

```bash
./gradlew :benchmarks-api:publishToMavenLocal
```

builds the jar, sources jar, Javadoc jar and POM into `~/.m2` without uploading anything.
