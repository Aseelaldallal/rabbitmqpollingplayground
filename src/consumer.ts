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

  let orderBulkOpInitiatedQueue: amqp.Replies.AssertQueue;
  try {
    orderBulkOpInitiatedQueue = await channel.assertQueue(
      "orderBulkOpInitiatedQueue",
      {
        exclusive: true,
      }
    );
  } catch (e) {
    console.log("Could not create queue ", e);
    process.exit();
  }

  await channel.bindQueue(
    orderBulkOpInitiatedQueue.queue,
    bulkOpExchange,
    "order"
  );

  const pollingExchange = "polling_exchange";
  channel.assertExchange(pollingExchange, "direct", {
    durable: false,
  });

  let queue5000: amqp.Replies.AssertQueue;
  try {
    queue5000 = await channel.assertQueue(`queue_5000`, {
      messageTtl: 5000,
      deadLetterExchange: bulkOpExchange,
    });
    await channel.bindQueue(queue5000.queue, pollingExchange, "5000");
  } catch (e) {
    console.log("Could not create queue5 ", e);
    process.exit();
  }

  let queue10000: amqp.Replies.AssertQueue;
  try {
    queue10000 = await channel.assertQueue(`queue_10000`, {
      messageTtl: 10000,
      deadLetterExchange: bulkOpExchange,
    });
    await channel.bindQueue(queue10000.queue, pollingExchange, "10000");
  } catch (e) {
    console.log("Could not create queue10 ", e);
    process.exit();
  }

  let queue15000: amqp.Replies.AssertQueue;
  try {
    queue15000 = await channel.assertQueue(`queue_15000`, {
      messageTtl: 15000,
      deadLetterExchange: bulkOpExchange,
    });
    await channel.bindQueue(queue15000.queue, pollingExchange, "15000");
  } catch (e) {
    console.log("Could not create queue15 ", e);
    process.exit();
  }

  let queue20000: amqp.Replies.AssertQueue;
  try {
    queue20000 = await channel.assertQueue(`queue_20000`, {
      messageTtl: 20000,
      deadLetterExchange: bulkOpExchange,
    });
    await channel.bindQueue(queue20000.queue, pollingExchange, "20000");
  } catch (e) {
    console.log("Could not create queue20 ", e);
    process.exit();
  }

  channel.consume(orderBulkOpInitiatedQueue.queue, (msg) => {
    if (!msg) {
      console.log("Error no msg");
      process.exit();
    }
    console.log("--------------------------------");
    console.log(
      `Recieved Msg: \n Routing Key: ${
        msg?.fields.routingKey
      } \n Content: ${msg?.content.toString()}`
    );
    console.log("msg", msg);

    if (msg.fields.routingKey === "queue_2000") {
      console.log(
        "Dropping Msg because it cycled through all dead letter queues"
      );
      process.exit();
    }

    const random = Math.random() * 10;
    const bulkOperationStatus = random > 5 ? "complete" : "incomplete";
    console.log("Bulk Operation Status: ", bulkOperationStatus);

    if (bulkOperationStatus === "incomplete") {
      console.log("Will Forward to Polling Exchange");
      const ttl = TTLCalc.calculateTTL();
      console.log("Polling Exchange will fwd to queue ", ttl);
      channel.publish(pollingExchange, `queue_${ttl}`, msg.content);
    }
  });
};

consume();
