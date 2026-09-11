package co.kademi.sync.commands;

import picocli.CommandLine.Option;

/**
 * The options every command takes, whatever it does.
 *
 * Mixed in rather than inherited so a command declares what it accepts, and so this file is the
 * one place any of these is described.
 */
public class GlobalOptions {

    @Option(names = {"-d", "--debug"},
            description = "Verbose output: show debug logging, with the level and source class on each line")
    public boolean debug;

    @Option(names = {"--logformat"},
            description = "Shape of each log line: plain (the message alone, the default), ts (an ISO-8601 UTC timestamp and level first) or kv (ts=.. level=.. msg=\"..\", for a log reader)")
    public co.kademi.sync.LogFormat logformat;

    @Option(names = {"--appdir"}, hidden = true,
            description = "Directory to work in. Set by a ksync:// link, not by hand")
    public String appdir;

    @Option(names = {"--appname"}, hidden = true,
            description = "App folder to create under -appdir. Required when -appdir is given")
    public String appname;
}
