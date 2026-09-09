# Wrapper scripts

Thin wrappers around `java -jar ksync3.jar`, so a day of ksync is `ksync-sync pull` rather than a
line of jvm arguments. They run the jar in [../dist](../dist), which is the one committed here, so
a `git pull` updates the tool and the wrappers together.

For a clone, in other words. Anyone who just wants to use ksync should take the installers in
[../installers](../installers) instead, which fetch the jar and a jre and put a `ksync3` command
on the PATH without a clone or a build. These wrappers add the defaults a day of development
wants on top of that: the command defaults to `sync`, and `-localwins` is on unless it is turned
off.

| Script | For |
|---|---|
| [ksync-sync.sh](ksync-sync.sh) | macOS, Linux, and Git Bash or WSL on Windows |
| [ksync-sync.bat](ksync-sync.bat) | Windows, from both cmd and PowerShell |
| [add-to-path.ps1](add-to-path.ps1) / [add-to-path.bat](add-to-path.bat) | puts this folder on a Windows PATH |

Both wrappers take the same shapes. The command defaults to `sync`, and a first argument that is
not a flag replaces it:

```
ksync-sync                    # sync
ksync-sync pull               # pull instead
ksync-sync pull -debug        # extra flags go straight through to ksync
ksync-sync -notray -debug     # still a sync: flags are not commands
```

ksync works on the directory you are standing in, so `cd` to the checkout first. Neither wrapper
changes directory, and neither needs to be told where the checkout is.

`-localwins` is passed by default to `sync`, `push` and `pull` - the three commands that resolve
conflicts. For sync and push the local checkout overwrites the remote even where the remote has
changed; for pull a remote change to a file you have edited is dropped in favour of yours. Either
way nothing is prompted, which is what a git managed checkout wants and the wrong thing wherever
the server is the source of truth. Set `KSYNC_LOCALWINS=0` to be asked about conflicts instead,
which is the plain ksync default.

Set `KSYNC_JAR` to run a jar from somewhere else, such as one you have just built.

## Putting them on your PATH

### Windows

Run it once, from this folder:

```
add-to-path.bat
```

Open a new terminal, and `ksync-sync` works from any checkout, in cmd and in PowerShell alike.
`add-to-path.bat -Remove` undoes it.

It adds this folder to your own PATH, so it needs no administrator, and because the folder is
inside the clone the wrappers stay current with `git pull`. It edits only your half of the PATH:
the usual `setx PATH "%PATH%;..."` advice truncates anything past 1024 characters and copies every
machine wide entry into your personal PATH, where those copies then shadow later system changes.
[../installers/install.cmd](../installers/install.cmd) does the same job for the `ksync3` command
it installs, by the same route, so a machine can end up with both on its PATH quite happily.

If you would rather not run a script, the same thing by hand: Start, "Edit environment variables
for your account", select Path, Edit, New, and paste the full path to this folder.

### macOS and Linux

Add the folder to your shell's PATH, in `~/.bashrc` or `~/.zshrc`:

```sh
export PATH="$PATH:$HOME/proj/ksync/scripts"
```

Or, if you already have a personal bin folder on your PATH, link it there instead, which keeps
one copy and follows the clone:

```sh
ln -s "$HOME/proj/ksync/scripts/ksync-sync.sh" "$HOME/.local/bin/ksync-sync"
```

## Requirements

Java on the PATH, or `JAVA_HOME` pointing at a jdk. Both wrappers check, and say which is missing
rather than failing somewhere further in.
