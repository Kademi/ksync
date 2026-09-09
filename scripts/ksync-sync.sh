#!/usr/bin/env bash
#
# Runs a ksync command on whatever directory you are standing in, with local as the authority.
#
# ksync takes the checkout from the jvm's working directory, so there is nothing to pass in: cd to
# the checkout and run this. The command defaults to sync; a first argument that is not a flag
# replaces it. Everything else goes straight through, so all of these work:
#
#   ksync-sync.sh                     # sync
#   ksync-sync.sh pull                # pull instead
#   ksync-sync.sh pull -debug
#   ksync-sync.sh push
#   ksync-sync.sh sync -url https://site/repos/myrepo   # follow a repo's latest version
#   ksync-sync.sh -notray -debug      # still a sync: flags are not commands
#   KSYNC_LOCALWINS=0 ksync-sync.sh   # ask about conflicts instead, the ksync default
#
# -localwins is passed by default to the three commands that resolve conflicts. For sync and push
# that means the local checkout overwrites the remote even where the remote has changed; for pull
# it means a remote change to a file you have edited is dropped in favour of yours. Either way
# nothing is prompted. That is what you want for a checkout that git manages, and it is the wrong
# thing anywhere the server is the source of truth.
#
# Override the jar with KSYNC_JAR=/path/to/ksync3.jar

set -euo pipefail

# The commands ksync registers. ksync itself is the authority - if you add one there, add it here
# too. Checked up front because ksync answers an unknown command by printing its usage and exiting
# 0, which a caller cannot tell apart from a command that ran.
KNOWN_COMMANDS=(sync pull push checkout verify login ignore publish usage)

COMMAND=sync
# A flag starts with a dash and a command does not, which is the whole rule, so a bare
# invocation and one carrying extra flags both still mean sync.
if [ $# -gt 0 ] && [ "${1#-}" = "$1" ]; then
    COMMAND=$1
    shift
fi

known=0
for c in "${KNOWN_COMMANDS[@]}"; do
    if [ "$c" = "$COMMAND" ]; then
        known=1
        break
    fi
done
if [ "$known" = 0 ]; then
    echo "Unknown ksync command: $COMMAND" >&2
    echo "Use one of: ${KNOWN_COMMANDS[*]}" >&2
    exit 2
fi

# Where this script is, so the jar beside it can be found. Read with shell builtins rather than
# dirname, because a desktop launcher or a cron job can hand this a PATH with nothing on it, and
# resolved through a symlink where the platform can do it, because linking this into a bin folder
# is a normal way to install it.
SELF="${BASH_SOURCE[0]}"
if command -v readlink >/dev/null 2>&1; then
    SELF="$(readlink -f "$SELF" 2>/dev/null || printf '%s' "$SELF")"
fi
HERE="${SELF%/*}"
if [ "$HERE" = "$SELF" ]; then
    HERE=. # invoked by a bare name in this directory, so there was no path to strip
fi
HERE="$(cd "$HERE" && pwd)"

# The jar that ships beside this script, so a clone works wherever it is put and a git pull
# updates both together. KSYNC_JAR wins, for running a jar you have just built.
JAR="${KSYNC_JAR:-$HERE/../dist/ksync3.jar}"

if [ ! -f "$JAR" ]; then
    echo "ksync jar not found at $JAR" >&2
    echo "Build one with: cd $HERE/../ksync-cli && mvn package" >&2
    echo "Or point KSYNC_JAR at an existing jar." >&2
    exit 1
fi

# A desktop launcher or a cron job gets a bare PATH, which often has no java on it even where an
# interactive shell does, so look in the two places it usually is before giving up.
JAVA=java
if ! command -v "$JAVA" >/dev/null 2>&1; then
    if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
        JAVA="$JAVA_HOME/bin/java"
    elif [ -x "$HOME/.sdkman/candidates/java/current/bin/java" ]; then
        # sdkman puts its shims on PATH for interactive shells only
        JAVA="$HOME/.sdkman/candidates/java/current/bin/java"
    else
        echo "No java on PATH, and JAVA_HOME is not set to a jdk" >&2
        exit 1
    fi
fi

# checkout and login are how a checkout comes into being, so for those the absence is expected.
if [ ! -d .ksync ] && [ "$COMMAND" != "checkout" ] && [ "$COMMAND" != "login" ]; then
    echo "Note: no .ksync in $(pwd), so this is not a checkout yet - ksync will ask for a url." >&2
fi

# Only the commands that resolve conflicts are given it. Passing it to login or verify would do
# nothing, and saying "local wins" on the way into them would be a lie about what is happening.
LOCALWINS=()
case "$COMMAND" in
    sync | push | pull)
        # On unless it is explicitly turned off, so a typo in the variable name cannot silently
        # leave the remote authoritative when this script says it is not.
        if [ "${KSYNC_LOCALWINS:-1}" != "0" ]; then
            LOCALWINS=(-localwins)
            echo "Running ksync $COMMAND in $(pwd) - local wins, remote changes are discarded"
        else
            echo "Running ksync $COMMAND in $(pwd) - conflicts will be asked about"
        fi
        ;;
    *)
        echo "Running ksync $COMMAND in $(pwd)"
        ;;
esac

# exec, so ctrl-c goes to the jvm instead of to this shell. A sync runs until it is killed, and
# its shutdown hook is what removes the status icon and writes the final status file.
exec "$JAVA" -jar "$JAR" -command "$COMMAND" "${LOCALWINS[@]}" "$@"
