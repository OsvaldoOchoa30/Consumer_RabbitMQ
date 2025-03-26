package main

import (
    "encoding/json"
    "log"

    "github.com/go-resty/resty/v2"
    amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
    if err != nil {
        log.Panicf("%s: %s", msg, err)
    }
}

func main() {
    client := resty.New()

    conn, err := amqp.Dial("amqp://osvaldo8a:OsvaOK30@13.216.98.244:5672/")
    failOnError(err, "Failed to connect to RabbitMQ")
    defer conn.Close()

    ch, err := conn.Channel()
    failOnError(err, "Failed to open a channel")
    defer ch.Close()

    err = ch.ExchangeDeclare(
        "logs",   // name
        "fanout", // type
        true,     // durable
        false,    // auto-deleted
        false,    // internal
        false,    // no-wait
        nil,      // arguments
    )
    failOnError(err, "Failed to declare an exchange")

    q, err := ch.QueueDeclare(
        "myConsumer", // name
        false,        // durable
        false,        // delete when unused
        true,         // exclusive
        false,        // no-wait
        nil,          // arguments
    )
    failOnError(err, "Failed to declare a queue")

    err = ch.QueueBind(
        q.Name, // queue name
        "",     // routing key
        "logs", // exchange
        false,
        nil,
    )
    failOnError(err, "Failed to bind a queue")

    msgs, err := ch.Consume(
        q.Name, // queue
        "",     // consumer
        false,  // auto-ack
        false,  // exclusive
        false,  // no-local
        false,  // no-wait
        nil,    // args
    )
    failOnError(err, "Failed to register a consumer")

    var forever chan struct{}

    go func() {
        for d := range msgs {
            log.Printf(" [x] Received message: %s", d.Body)

            // Estructura para decodificar el JSON recibido
            var requestData map[string]interface{}
            err := json.Unmarshal(d.Body, &requestData)
            if err != nil {
                log.Printf("Error al decodificar JSON: %s", err)
                continue
            }

            // Extraer solo el id_payment
            idPayment, ok := requestData["id"].(float64)
            if !ok {
                log.Printf("id_payment no encontrado en el mensaje recibido")
                continue
            }

            // Crear el JSON con solo el id_payment
            requestBody, err := json.Marshal(map[string]interface{}{
                "id_reservation": int(idPayment), // Convertir a entero
            })
            if err != nil {
                log.Printf("Error al crear JSON de id_payment: %s", err)
                continue
            }

            // Imprimir antes de enviar
            log.Printf("Enviando request POST con los siguientes detalles:")
            log.Printf("URL: http://localhost:9090/v1/payment/create")
            log.Printf("Headers: Content-Type: application/json")
            log.Printf("Body: %s", requestBody)

            for i := 0; i < 3; i++ {
                resp, err := client.R().
                    SetHeader("Content-Type", "application/json").
                    SetBody(requestBody).
                    Post("http://localhost:9090/v1/payment/create")

                if err == nil && resp.StatusCode() < 500 {
                    log.Printf("Request exitoso. Código de estado: %d", resp.StatusCode())
                    break
                }

                log.Printf("Intento %d falló, reintentando...", i+1)
            }
        }
    }()

    log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
    <-forever
}
