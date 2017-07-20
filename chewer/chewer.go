package main

import (
"bufio"
"fmt"
"log"
"os"
"strings"
"encoding/json"

"github.com/streadway/amqp"
"github.com/Jeffail/gabs"
)



type Configuration struct {
    ValidCommerceIds    []string
    MqServer            []string
}

func main() {

    config, _ := os.Open(os.Args[1])
    decoder := json.NewDecoder(config)
    configuration := Configuration{}
    err := decoder.Decode(&configuration)
    if err != nil {
      fmt.Println("error:", err)
  }



  file, err := os.Open(os.Args[2])
  if err != nil {
    log.Fatal(err)
}
defer file.Close()

fileScanner := bufio.NewScanner(file)
lineCount := 0
for fileScanner.Scan() {
    line := strings.Fields(fileScanner.Text())
    if(stringInArray(line[0], configuration.ValidCommerceIds)) {  
        lineCount++
    }
}
fmt.Println("total ", lineCount)


conn, err := amqp.Dial("amqp://rAdmin:radmin3357@192.168.10.222:5672/")
failOnError(err, "Failed to connect to RabbitMQ")
defer conn.Close()

ch, err := conn.Channel()
failOnError(err, "Failed to open a channel")
defer ch.Close()



q, err := ch.QueueDeclare(
  "cupon", // name
  false,   // durable
  false,   // delete when unused
  false,   // exclusive
  false,   // no-wait
  nil,     // arguments
)
failOnError(err, "Failed to declare a queue")


file2, err := os.Open(os.Args[2])
if err != nil {
    log.Fatal(err)
}
defer file2.Close()

scanner := bufio.NewScanner(file2)
scanner.Scan()
headers := strings.Fields(scanner.Text())  

current := 0

for scanner.Scan() {
    entry := strings.Fields(scanner.Text())
    if(stringInArray(entry[0], configuration.ValidCommerceIds)) {  
        jsonObj := gabs.New()
        for i, v := range entry {
            jsonObj.Set(v,headers[i])
        }

            //Publicamos el mensaje:
        body := jsonObj.String()
        err = ch.Publish(
            "",     // exchange
            q.Name, // routing key
            false,  // mandatory
            false,  // immediate
            amqp.Publishing {
                ContentType: "text/plain",
                Body:        []byte(body),
                })
        current++
        fmt.Println("current ", current)

        failOnError(err, "Failed to publish a message")


    }
}

if err := scanner.Err(); err != nil {
    log.Fatal(err)
}
}

func stringInArray(str string, list []string) bool {
    for _, v := range list {
        if v == str {
            return true
        }
    }
    return false
}

func failOnError(err error, msg string) {
  if err != nil {
    log.Fatalf("%s: %s", msg, err)
    panic(fmt.Sprintf("%s: %s", msg, err))
}
}