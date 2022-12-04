package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) InsertServer(addr string) {
	c.ServerMap[c.Hash(addr)] = addr
}

func (c ConsistentHashRing) DeleteServer(addr string) {
	delete(c.ServerMap, c.Hash(addr))
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// find the next largest key from ServerMap
	minServer := ""
	minServerHash := ""

	closestServer := ""
	closestServerHash := ""
	for key, val := range c.ServerMap {
		if minServer == "" || minServerHash > key {
			minServer = val
			minServerHash = key
		}
		if blockId < key && (closestServer == "" || key < closestServerHash) {
			closestServer = val
			closestServerHash = key
		}
	}
	if closestServer == "" {
		closestServer = minServer
		closestServerHash = minServerHash
	}
	return closestServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func (c ConsistentHashRing) OutputMap(blockHashes []string) map[string]string {
	res := make(map[string]string)
	for i := 0; i < len(blockHashes); i++ {
		res[blockHashes[i]] = c.GetResponsibleServer(blockHashes[i])
	}
	return res
}

func NewConsistentHashRing(numServers int, downServer []int) *ConsistentHashRing {
	c := &ConsistentHashRing{
		ServerMap: make(map[string]string),
	}

	for i := 0; i < numServers; i++ {
		c.InsertServer("blockServer" + strconv.Itoa(i))
	}

	for i := 0; i < len(downServer); i++ {
		c.DeleteServer("blockServer" + strconv.Itoa(downServer[i]))
	}

	return c
}
