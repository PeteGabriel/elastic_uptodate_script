package main

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Environment struct {
	ExternalClusterName string `mapstructure:"EXTERNAL_CLUSTER_NAME"`
	ExternalUsername    string `mapstructure:"EXTERNAL_USERNAME"`
	ExternalPassword    string `mapstructure:"EXTERNAL_PASSWORD"`
	ExternalURI         string `mapstructure:"EXTERNAL_URI"`

	InternalClusterName string `mapstructure:"INTERNAL_CLUSTER_NAME"`
	InternalUsername    string `mapstructure:"INTERNAL_USERNAME"`
	InternalPassword    string `mapstructure:"INTERNAL_PASSWORD"`
	InternalURI         string `mapstructure:"INTERNAL_URI"`
}

func New(envPath string) (*Environment, error) {
	var env Environment
	viper.SetConfigFile(envPath)
	viper.SetConfigType("env")
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		return nil, errors.Wrap(err, "No env file found")
	}
	//try to assign read variables into golang struct
	if err := viper.Unmarshal(&env); err != nil {
		return nil, errors.Wrap(err, "Error trying to unmarshal configuration")
	}
	return &env, nil
}