package config

import (
	"log"
	"github.com/spf13/viper"
)

type Config struct {
	GRPCPort      string `mapstructure:"GRPC_PORT"`
	DatabaseURL   string `mapstructure:"DATABASE_URL"`
}

var AppConfig *Config

func LoadConfig() {
	viper.AddConfigPath(".")
	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Println("Warning: .env file not found, reading from environment variables")
	}
	
	err := viper.Unmarshal(&AppConfig)
	if err != nil {
		log.Fatalf("Unable to decode config into struct, %v", err)
	}
}
