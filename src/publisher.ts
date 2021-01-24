// Creates Bulk Operation

import * as amqp from "amqplib";

const publish = async () => {
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

  const exchange = "bulk_operation_exchange";
  channel.assertExchange(exchange, "direct", {
    durable: true,
  });

  const msg = {
    text: "Started Bulk Operation",
    date: new Date(),
    account: "shopify+1789",
  };

  channel.publish(exchange, "order", Buffer.from(JSON.stringify(msg)));
  console.log("Published Msg: ", msg);

  setTimeout(() => {
    connection.close();
    process.exit(0);
  }, 500);
};

publish();
