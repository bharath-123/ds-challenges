package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

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

type BroadcastRequest struct {
	Source string        `json:"src"`
	Dest   string        `json:"dst"`
	Body   BroadcastBody `json:"body"`
}

type BulkBroadcastRequest struct {
	Source string            `json:"src"`
	Dest   string            `json:"dst"`
	Body   BulkBroadcastBody `json:"body"`
}

type BulkBroadcastBody struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
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
	messageMutex := sync.RWMutex{}
	messages := map[int]bool{}
	networkTopology := NetworkTopology{Nodes: map[string][]string{}}

	n.Handle("bulk_broadcast", func(msg maelstrom.Message) error {
		var body BulkBroadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messageMutex.Lock()
		for _, v := range body.Messages {
			messages[v] = true
		}
		messageMutex.Unlock()

		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body BroadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messageMutex.Lock()
		messages[body.Message] = true
		messageMutex.Unlock()

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
		messageMutex.RLock()
		for k, _ := range messages {
			messageRes = append(messageRes, k)
		}
		messageMutex.RUnlock()

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

	go func() {
		for {
			nodeIds := n.NodeIDs()
			messagesToBroadcast := []int{}
			messageMutex.RLock()
			for k, _ := range messages {
				messagesToBroadcast = append(messagesToBroadcast, k)
			}
			messageMutex.RUnlock()
			for _, id := range nodeIds {
				err := n.Send(id, BulkBroadcastBody{
					Type:     "bulk_broadcast",
					Messages: messagesToBroadcast,
				})
				if err != nil {
					log.Printf("reply error: %s", err)
				}
			}

			time.Sleep(5 * time.Second)
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
