package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const KEY_NAME = "key"

type AddBody struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type AddResponse struct {
	Type string `json:"type"`
}

type ReadBody struct {
	Type string `json:"type"`
}

type ReadResponse struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

type OplogResponse struct {
	Type string `json:"type"`
}

type OplogRequest struct {
	Type  string `json:"type"`
	Oplog []int  `json:"oplog"`
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	opLogMutex := sync.RWMutex{}
	opLog := []int{}

	n.Handle("add", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body AddBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val, err := kv.ReadInt(context.Background(), KEY_NAME)
		errCode := maelstrom.ErrorCode(err)
		if errCode == 20 {
			val = 0
		} else {
			return err
		}

		newVal := val + body.Delta

		opLogMutex.Lock()
		opLog = append(opLog, body.Delta)
		opLogMutex.Unlock()

		err = kv.Write(context.Background(), KEY_NAME, newVal)
		if err != nil {
			return err
		}

		response := AddResponse{Type: "add_ok"}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body ReadBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		val, err := kv.ReadInt(ctx, KEY_NAME)
		errCode := maelstrom.ErrorCode(err)
		if errCode == 20 {
			val = 0
		} else {
			return err
		}

		resp := ReadResponse{
			Type:  "read_ok",
			Value: val,
		}

		return n.Reply(msg, resp)

	})

	n.Handle("recv_oplog", func(msg maelstrom.Message) error {
		var body OplogRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		rcvdOplog := body.Oplog
		currOplogLen := 0

		opLogMutex.RLock()
		currOplogLen = len(opLog)
		opLogMutex.RUnlock()

		if currOplogLen > len(rcvdOplog) {
			return nil
		}

		newValue := 0
		for i := currOplogLen; i < len(rcvdOplog); i++ {
			newValue += rcvdOplog[i]
		}

		currentValue, err := kv.ReadInt(context.Background(), KEY_NAME)
		if err != nil {
			return err
		}

		if newValue < currentValue {
			return nil
		}

		err = kv.Write(context.Background(), KEY_NAME, newValue)
		if err != nil {
			return err
		}

		return nil
	})

	go func() {
		for {
			nodes := n.NodeIDs()
			currNodeId := n.ID()
			for _, node := range nodes {
				if node == currNodeId {
					continue
				}

				opLogMutex.RLock()
				request := OplogRequest{
					Type:  "recv_oplog",
					Oplog: opLog,
				}
				opLogMutex.RUnlock()

				err := n.Send(node, request)
				if err != nil {
					log.Printf("error sending oplog to node %s: %s", node, err)
				}
			}

			time.Sleep(3 * time.Second)
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
