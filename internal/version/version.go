package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

var (
	Version = ""
	Commit  = ""
	Date    = ""
)

const (
	devVersion        = "dev"
	unknown           = "unknown"
	shortCommitLength = 12
)

type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	Date      string `json:"date"`
	GoVersion string `json:"go_version"`
	Platform  string `json:"platform"`
}

func Get() Info {
	info := Info{
		Version:   Version,
		Commit:    Commit,
		Date:      Date,
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}

	buildVersion, buildCommit, buildDate := readBuildInfo()
	if info.Version == "" {
		info.Version = buildVersion
	}
	if info.Commit == "" {
		info.Commit = buildCommit
	}
	if info.Date == "" {
		info.Date = buildDate
	}

	return info
}

func String() string {
	info := Get()

	return fmt.Sprintf(
		"fq %s (%s, %s) %s %s",
		info.Version,
		shortCommit(info.Commit),
		info.Date,
		info.GoVersion,
		info.Platform,
	)
}

func Requested(args []string) bool {
	for _, arg := range args {
		switch arg {
		case "-version", "--version", "version":
			return true
		}
	}

	return false
}

func readBuildInfo() (version, commit, date string) {
	version, commit, date = devVersion, unknown, unknown

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version, commit, date
	}

	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		version = info.Main.Version
	}

	modified := false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			commit = setting.Value
		case "vcs.time":
			date = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}

	if modified && commit != unknown {
		commit += "-dirty"
	}

	return version, commit, date
}

func shortCommit(commit string) string {
	if len(commit) <= shortCommitLength {
		return commit
	}

	return commit[:shortCommitLength]
}
