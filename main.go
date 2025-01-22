package main

import (
	"flag"
	"fmt"
	"godis/lib/logger"
	"godis/tcp"

	"godis/config"

	"godis/redis/server"
)

var configFilename string
var defaultconfigFileName = "config.yaml"

const banner = `
   ________   _______    ________   ___   ________      
  |\   __  \ |\  ___ \  |\   ___ \ |\  \ |\   ____\     
  \ \  \|\  \\ \   __/| \ \  \_|\ \\ \  \\ \  \___|_    
   \ \   _  _\\ \  \_|/__\ \  \ \\ \\ \  \\ \_____  \   
    \ \  \\  \|\ \  \_|\ \\ \  \_\\ \\ \  \\|____|\  \  
     \ \__\\ _\ \ \_______\\ \_______\\ \__\ ____\_\  \ 
      \|__|\|__| \|_______| \|_______| \|__||\_________\
                                            \|_________|
`

func main() {
	// ListenAndServe(":8000")
	flag.StringVar(&configFilename, "f", defaultconfigFileName, "the config file")
	flag.Parse()

	config.SetupConfig(configFilename)

	if err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, server.MakeHandler()); err != nil {
		logger.Error(err)
	}
}
