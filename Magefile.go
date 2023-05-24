//go:build mage
// +build mage

package main

import (
	// mage:import
	build "github.com/grafana/grafana-plugin-sdk-go/build"
	"github.com/magefile/mage/mg"
)

func BuildCustom() {
	b := build.Build{}
	// same as build.BuildAll, but without Windows and LinuxARM - apache/arrow/go/v10 has problems with compilation on that platforms
	mg.Deps(b.Linux, b.Windows, b.Darwin, b.DarwinARM64, b.LinuxARM64, b.GenerateManifestFile)
}

// Default configures the default target.
var Default = BuildCustom
