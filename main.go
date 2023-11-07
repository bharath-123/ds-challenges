package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NetworkTopology struct {
	Nodes map[string][]string `json:"nodes"`
}

type NetworkTopologyBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type NetworkTopologyResponse struct {
	Type string `json:"type"`
}

type BroadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type BroadcastResponse struct {
	Type string `json:"type"`
}

type ReadBody struct {
	Type string `json:"type"`
}

type ReadResponse struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

func main() {
	n := maelstrom.NewNode()
	messages := map[int]bool{}
	networkTopology := NetworkTopology{Nodes: map[string][]string{}}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body BroadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages[body.Message] = true

		// Update the message type to return back.
		response := BroadcastResponse{Type: "broadcast_ok"}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body ReadBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messageRes := []int{}
		for k, _ := range messages {
			messageRes = append(messageRes, k)
		}

		// Update the message type to return back.
		response := ReadResponse{
			Type:     "read_ok",
			Messages: messageRes,
		}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body NetworkTopologyBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		networkTopology.Nodes = body.Topology

		// Update the message type to return back.
		response := NetworkTopologyResponse{Type: "topology_ok"}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
