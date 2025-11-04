package info_test

import (
	"log"
	"testing"

	"github.com/ttn-nguyen42/gedis/gedis/info"
)

func TestInfoString(t *testing.T) {
	infoObj := info.NewInfo("6.2.5")
	infoObj.Replication = &info.Replication{
		Role:            "master",
		ConnectedSlaves: 2,
		MasterReplID:    "abcd1234",
	}
	log.Println(infoObj.String())
}
