package clictlplanefuncs

import (
 "fmt"
 SD "github.com/00pauln00/niova-pumicedb/go/pkg/utils/servicediscovery"
)


/*
How should I model?
1. Get the data bytes
*/

type CLICtlplane struct {
 //Service discovery
 SDisObj SD. 
}


func (cli *CLICtlplane) ReadSnapForChunk(vdev string, chunk uint32) ([]uint64, error) {
  
}
