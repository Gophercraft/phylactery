# Gophercraft Phylactery

[![Go Reference](https://pkg.go.dev/badge/github.com/Gophercraft/phylactery.svg)](https://pkg.go.dev/github.com/Gophercraft/phylactery)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Chat on discord](https://img.shields.io/discord/556039662997733391.svg)](https://discord.gg/xPtuEjt)

Phylactery is an embeddedable NoSQL database library for Go applications.

Currently the only storage engine for Phylactery is based on [LevelDB](https://github.com/syndtr/goleveldb), an efficient key-value store. The API is somewhat inspired by existing Go SQL ORMs.

It's not recommended that you use Phylactery outside of its intended scope of Gophercraft, I cannot guarantee that your data won't be lost.
