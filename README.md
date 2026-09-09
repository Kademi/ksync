# kapp-cli
KSync is a file sync tool for working with Kademi

## Install
Installs `ksync3.jar`, a Java runtime if no Java 11+ is found, and a `ksync3` command.

**macOS, Linux, WSL** (add `sudo` for a system-wide install into /usr/local):
```bash
curl -fsSL https://raw.githubusercontent.com/Kademi/ksync/master/installers/install.sh | bash
```

**Windows PowerShell:**
```powershell
irm https://raw.githubusercontent.com/Kademi/ksync/master/installers/install.ps1 | iex
```

**Windows CMD:**
```batch
curl -fsSL https://raw.githubusercontent.com/Kademi/ksync/master/installers/install.cmd -o install.cmd && install.cmd && del install.cmd
```

Then, from your project folder:
```sh
ksync3 -command sync
```

Set `KSYNC3_BUNDLE_JRE=1` to always install a private JRE even when Java is present. Re-run the installer to update.

Where it goes:

| Platform | Jar and JRE | Launcher |
| --- | --- | --- |
| Linux, WSL | `~/.local/share/ksync3` (or `$XDG_DATA_HOME/ksync3`) | `~/.local/bin/ksync3` |
| macOS | `~/Library/Application Support/ksync3` | `~/.local/bin/ksync3` |
| Linux or macOS with `sudo` | `/usr/local/lib/ksync3` | `/usr/local/bin/ksync3` |
| Windows | `%LOCALAPPDATA%\Programs\ksync3` | `%LOCALAPPDATA%\Programs\ksync3\bin\ksync3.cmd` |

Uninstall on Linux or macOS by deleting those two paths, on Windows by running `uninstall.cmd` in the install folder. Override with `KSYNC3_HOME` and, on Linux or macOS, `KSYNC3_BIN`.

## Wrapper scripts
If you have a clone of this repo, [scripts](./scripts) holds thin wrappers that run the jar in
[dist](./dist) - `ksync-sync pull` rather than a line of jvm arguments - for macOS, Linux and
Windows, with a one line way to get them onto a Windows PATH. The installers above are the way in
for everyone else. See [scripts/README.md](./scripts/README.md).

## Documentation
Please read at [here](https://docs.kademi.co/blogs/docs-kb/developing-with-ksync/)

## License
Please read at [here](./LICENSE.md)
