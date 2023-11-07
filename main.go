package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "generate_ok"

		// To be unique across distributed nodes, the id type will be a number constructed in the following way:
		// timestamp + node id + random number b/w 0 and 1000000
		body["id"] = strconv.Itoa(int(time.Now().UnixNano())) + n.ID() + strconv.Itoa(rand.Intn(1000000))

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
