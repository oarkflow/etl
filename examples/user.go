package main

import (
	"examples/utils"
)

// main loads the configuration file and migrates all tables.
func main() {
	err := utils.ProcessFile("users.yaml")
	if err != nil {
		panic(err)
	}
}
