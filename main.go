package main

import (
	"context"
	"encoding/json"
	"log"

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

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

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

		err = kv.CompareAndSwap(context.Background(), KEY_NAME, val, newVal, true)
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

		val, err := kv.ReadInt(context.Background(), KEY_NAME)
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

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
