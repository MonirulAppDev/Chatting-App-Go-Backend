package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// setup client
type Client struct {
	UserID string
	Conn   *websocket.Conn
}

var clients = make(map[string]*Client)
var mu sync.Mutex

// convert http request to ws
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// message
type Message struct {
	To      string `json:"to"`
	From    string `json:"from"`
	Content string `json:"content"`
}

// message handler
func MessageHandler(c *gin.Context) {
	userId := c.Query("user_id")

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("Upgrader Error: ", err)
		return
	}

	client := &Client{
		UserID: userId,
		Conn:   conn,
	}

	mu.Lock()
	clients[userId] = client
	mu.Unlock()

	log.Printf("User connected: %s | Total users: %d\n", userId, len(clients))

	for {
		var msg Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			log.Println("Read error: ", err)
			mu.Lock()
			delete(clients, userId)
			mu.Unlock()

			log.Println("User disconnected: ", userId)
			break
		}

		log.Printf("[RECEIVED] From: %s, To: %s, Content: %s\n", msg.From, msg.To, msg.Content)
		SendMessage(msg)
	}
}

func SendMessage(msg Message) {
	mu.Lock()
	receiver, ok := clients[msg.To]
	mu.Unlock()

	if ok {
		err := receiver.Conn.WriteJSON(msg)
		if err != nil {
			log.Println("Write error: ", err)
		}
		log.Printf("[SENT] From: %s, To: %s, Content: %s\n", msg.From, msg.To, msg.Content)
	} else {
		log.Printf("[NOT SENT] User %s not connected. From: %s, Content: %s\n", msg.To, msg.From, msg.Content)
	}
}

func main() {
	r := gin.Default()
	r.GET("/ws", MessageHandler)
	log.Println("Server running on :8080")
	r.Run(":8080")
}
