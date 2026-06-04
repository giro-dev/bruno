import amqplib from 'amqplib';

const safeParseJSON = (jsonString, context = 'JSON string') => {
  try {
    return JSON.parse(jsonString);
  } catch (error) {
    const errorMessage = `Failed to parse ${context}: ${error.message}`;
    console.error(errorMessage, { originalString: jsonString, parseError: error });
    throw new Error(errorMessage);
  }
};

const safeStringifyJSON = (obj) => {
  try {
    return JSON.stringify(obj, null, 2);
  } catch (error) {
    return String(obj);
  }
};

class AmqpClient {
  constructor(sendEvent) {
    this.sendEvent = sendEvent;
    this.connections = {};
    this.channels = {};
    this.consumers = {};
    this.messageQueues = {};
  }

  async connect(requestUid, collectionUid, url, options = {}) {
    const key = `${requestUid}:${collectionUid}`;

    try {
      await this.disconnect(requestUid, collectionUid);
    } catch (_) {
      // ignore cleanup errors
    }

    try {
      const connectOptions = {};
      if (options.heartbeat !== undefined && options.heartbeat > 0) {
        connectOptions.heartbeat = options.heartbeat;
      }

      const connection = await amqplib.connect(url, connectOptions);
      this.connections[key] = connection;

      connection.on('error', (err) => {
        this.sendEvent('main:amqp:error', requestUid, collectionUid, {
          message: err.message,
          timestamp: Date.now()
        });
      });

      connection.on('close', () => {
        this.sendEvent('main:amqp:disconnected', requestUid, collectionUid, {
          timestamp: Date.now()
        });
        delete this.connections[key];
        delete this.channels[key];
        delete this.consumers[key];
      });

      const channel = await connection.createChannel();
      this.channels[key] = channel;

      if (options.prefetch && options.prefetch > 0) {
        await channel.prefetch(options.prefetch);
      }

      channel.on('error', (err) => {
        this.sendEvent('main:amqp:error', requestUid, collectionUid, {
          message: `Channel error: ${err.message}`,
          timestamp: Date.now()
        });
      });

      channel.on('close', () => {
        delete this.channels[key];
        delete this.consumers[key];
      });

      this.sendEvent('main:amqp:connected', requestUid, collectionUid, {
        timestamp: Date.now()
      });

      return { success: true };
    } catch (err) {
      this.sendEvent('main:amqp:error', requestUid, collectionUid, {
        message: `Connection failed: ${err.message}`,
        timestamp: Date.now()
      });
      throw err;
    }
  }

  async declareExchange(requestUid, collectionUid, exchange, exchangeType = 'direct', options = {}) {
    const key = `${requestUid}:${collectionUid}`;
    const channel = this.channels[key];
    if (!channel) throw new Error('No channel available. Connect first.');

    await channel.assertExchange(exchange, exchangeType, {
      durable: options.durable !== false,
      ...options
    });
  }

  async declareQueue(requestUid, collectionUid, queue = '', options = {}) {
    const key = `${requestUid}:${collectionUid}`;
    const channel = this.channels[key];
    if (!channel) throw new Error('No channel available. Connect first.');

    const result = await channel.assertQueue(queue, {
      durable: options.durable !== false,
      exclusive: queue === '',
      ...options
    });

    return result;
  }

  async bindQueue(requestUid, collectionUid, queue, exchange, routingKey = '') {
    const key = `${requestUid}:${collectionUid}`;
    const channel = this.channels[key];
    if (!channel) throw new Error('No channel available. Connect first.');

    await channel.bindQueue(queue, exchange, routingKey);
  }

  async publish(requestUid, collectionUid, exchange, routingKey, content, options = {}) {
    const key = `${requestUid}:${collectionUid}`;
    const channel = this.channels[key];
    if (!channel) throw new Error('No channel available. Connect first.');

    let buffer;
    if (Buffer.isBuffer(content)) {
      buffer = content;
    } else if (typeof content === 'string') {
      buffer = Buffer.from(content, 'utf-8');
    } else {
      buffer = Buffer.from(safeStringifyJSON(content), 'utf-8');
    }

    const publishOptions = {
      persistent: options.persistent !== false,
      contentType: options.contentType || 'application/json',
      ...options
    };

    const result = channel.publish(exchange, routingKey, buffer, publishOptions);

    this.sendEvent('main:amqp:message-published', requestUid, collectionUid, {
      exchange,
      routingKey,
      content: buffer.toString('utf-8'),
      options: publishOptions,
      timestamp: Date.now(),
      confirmed: result
    });

    return result;
  }

  async sendToQueue(requestUid, collectionUid, queue, content, options = {}) {
    const key = `${requestUid}:${collectionUid}`;
    const channel = this.channels[key];
    if (!channel) throw new Error('No channel available. Connect first.');

    let buffer;
    if (Buffer.isBuffer(content)) {
      buffer = content;
    } else if (typeof content === 'string') {
      buffer = Buffer.from(content, 'utf-8');
    } else {
      buffer = Buffer.from(safeStringifyJSON(content), 'utf-8');
    }

    const sendOptions = {
      persistent: options.persistent !== false,
      contentType: options.contentType || 'application/json',
      ...options
    };

    const result = channel.sendToQueue(queue, buffer, sendOptions);

    this.sendEvent('main:amqp:message-published', requestUid, collectionUid, {
      queue,
      content: buffer.toString('utf-8'),
      options: sendOptions,
      timestamp: Date.now(),
      confirmed: result
    });

    return result;
  }

  async consume(requestUid, collectionUid, queue, options = {}) {
    const key = `${requestUid}:${collectionUid}`;
    const channel = this.channels[key];
    if (!channel) throw new Error('No channel available. Connect first.');

    // Cancel existing consumer if any
    if (this.consumers[key]) {
      try {
        await channel.cancel(this.consumers[key]);
      } catch (_) {
        // ignore
      }
    }

    const consumeResult = await channel.consume(
      queue,
      (msg) => {
        if (msg === null) {
          this.sendEvent('main:amqp:consumer-cancelled', requestUid, collectionUid, {
            timestamp: Date.now()
          });
          return;
        }

        const contentString = msg.content.toString('utf-8');
        let parsedContent = contentString;
        try {
          parsedContent = safeParseJSON(contentString, 'AMQP message');
          parsedContent = safeStringifyJSON(parsedContent);
        } catch (_) {
          // keep as string if not valid JSON
        }

        this.sendEvent('main:amqp:message-received', requestUid, collectionUid, {
          content: parsedContent,
          fields: {
            exchange: msg.fields.exchange,
            routingKey: msg.fields.routingKey,
            deliveryTag: msg.fields.deliveryTag,
            redelivered: msg.fields.redelivered,
            consumerTag: msg.fields.consumerTag
          },
          properties: {
            contentType: msg.properties.contentType,
            contentEncoding: msg.properties.contentEncoding,
            headers: msg.properties.headers,
            correlationId: msg.properties.correlationId,
            replyTo: msg.properties.replyTo,
            messageId: msg.properties.messageId,
            timestamp: msg.properties.timestamp,
            type: msg.properties.type,
            appId: msg.properties.appId
          },
          timestamp: Date.now()
        });

        // Auto-ack unless noAck is set
        if (!options.noAck) {
          channel.ack(msg);
        }
      },
      { noAck: options.noAck || false }
    );

    this.consumers[key] = consumeResult.consumerTag;

    this.sendEvent('main:amqp:consuming', requestUid, collectionUid, {
      queue,
      consumerTag: consumeResult.consumerTag,
      timestamp: Date.now()
    });

    return consumeResult;
  }

  async stopConsuming(requestUid, collectionUid) {
    const key = `${requestUid}:${collectionUid}`;
    const channel = this.channels[key];
    const consumerTag = this.consumers[key];

    if (channel && consumerTag) {
      await channel.cancel(consumerTag);
      delete this.consumers[key];

      this.sendEvent('main:amqp:consumer-stopped', requestUid, collectionUid, {
        timestamp: Date.now()
      });
    }
  }

  async disconnect(requestUid, collectionUid) {
    const key = `${requestUid}:${collectionUid}`;

    try {
      if (this.consumers[key] && this.channels[key]) {
        await this.channels[key].cancel(this.consumers[key]);
      }
    } catch (_) {
      // ignore
    }

    try {
      if (this.channels[key]) {
        await this.channels[key].close();
      }
    } catch (_) {
      // ignore
    }

    try {
      if (this.connections[key]) {
        await this.connections[key].close();
      }
    } catch (_) {
      // ignore
    }

    delete this.connections[key];
    delete this.channels[key];
    delete this.consumers[key];

    this.sendEvent('main:amqp:disconnected', requestUid, collectionUid, {
      timestamp: Date.now()
    });
  }

  async disconnectAll() {
    const keys = Object.keys(this.connections);
    for (const key of keys) {
      const [requestUid, collectionUid] = key.split(':');
      try {
        await this.disconnect(requestUid, collectionUid);
      } catch (_) {
        // ignore
      }
    }
  }

  closeForCollection(collectionUid) {
    const keys = Object.keys(this.connections).filter((k) => k.endsWith(`:${collectionUid}`));
    for (const key of keys) {
      const [requestUid, colUid] = key.split(':');
      this.disconnect(requestUid, colUid).catch(() => {});
    }
  }

  getStatus(requestUid, collectionUid) {
    const key = `${requestUid}:${collectionUid}`;
    return {
      connected: !!this.connections[key],
      hasChannel: !!this.channels[key],
      consuming: !!this.consumers[key]
    };
  }
}

export { AmqpClient };
