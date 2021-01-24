// Check if bulk operation is done

import * as amqp from "amqplib";
import * as TTLCalc from "./ttlCalc";

const consume = async () => {
  let connection: amqp.Connection;
  try {
    connection = await amqp.connect("amqp://localhost");
  } catch (e) {
    console.log("Could not connect", e);
    process.exit();
  }

  let channel: amqp.Channel;
  try {
    channel = await connection.createChannel();
  } catch (e) {
    console.log("Could not create channel", e);
    process.exit();
  }

  const bulkOpExchange = "bulk_operation_bulkOpExchange";
  channel.assertExchange(bulkOpExchange, "direct", {
    durable: true,
  });

  const pollingExchange = "polling_exchange";
  channel.assertExchange(pollingExchange, "direct", {
    durable: false,
  });

  let orderBulkOpInitiatedQueue: amqp.Replies.AssertQueue;
  try {
    orderBulkOpInitiatedQueue = await channel.assertQueue("orderBulkOpInitiatedQueue", {
      exclusive: true,
    });
  } catch (e) {
    console.log("Could not create queue ", e);
    process.exit();
  }

  channel.bindQueue(orderBulkOpInitiatedQueue.queue, bulkOpExchange, "order");

  channel.consume(orderBulkOpInitiatedQueue.queue, (msg) => {
    console.log(
      `Recieved Msg: \n Routing Key: ${
        msg?.fields.routingKey
      } \n Content: ${msg?.content.toString()}`
    );

    const random = Math.random() * 10;
    const bulkOperationStatus = random > 5 ? "complete" : "incomplete";
    console.log("Bulk Operation Status: ", bulkOperationStatus);

    if (bulkOperationStatus === "incomplete") {
      const ttl = TTLCalc.calculateTTL();
      let delayQueue: amqp.Replies.AssertQueue;
      try {
          delayQueue = await channel.assertQueue(`Queue.${ttl}`, {
              messageTtl: ttl,
              deadLetterExchange: pollingExchange,
              expires
          })
      }
    }
  });
};

consume();
