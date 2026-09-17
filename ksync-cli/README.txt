RELEASE PROCESS

1. Edit ksync-cli/pom.xml, change the project version (the one under <packaging>jar</packaging>, not the parent's).
2. mvn -f ksync-cli/pom.xml package
3. Commit pom.xml and dependency-reduced-pom.xml, then push.
4. git tag -a v1.8.13 -m "ksync3 1.8.13" then git push origin v1.8.13
5. Done. The tag starts the workflow, which builds and publishes the jar, the completion script and SHA256SUMS. Check https://github.com/Kademi/ksync/releases.