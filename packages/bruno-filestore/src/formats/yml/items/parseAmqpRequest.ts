import type { Item as BrunoItem } from '@usebruno/schema-types/collection/item';
import { uuid, ensureString } from '../../../utils';

const parseAmqpRequest = (ocRequest: any): BrunoItem => {
  const info = ocRequest.info;
  const amqp = ocRequest.amqp;

  const brunoRequest: any = {
    url: ensureString(amqp?.url),
    exchange: ensureString(amqp?.exchange),
    exchangeType: ensureString(amqp?.exchangeType, 'direct'),
    routingKey: ensureString(amqp?.routingKey),
    queue: ensureString(amqp?.queue),
    headers: [],
    body: {
      mode: 'amqp',
      amqp: []
    },
    docs: null
  };

  // message
  if (amqp?.message) {
    const messageData = ensureString(amqp.message.data);
    brunoRequest.body.amqp = [{
      name: 'message 1',
      type: amqp.message.type || 'json',
      content: messageData
    }];
  }

  // docs
  if (ocRequest.docs) {
    brunoRequest.docs = ocRequest.docs;
  }

  // settings
  const amqpSettings: Record<string, number | string> = {
    timeout: 0,
    heartbeat: 0,
    prefetch: 0,
    vhost: '/'
  };

  if (ocRequest.settings) {
    if (typeof ocRequest.settings.timeout === 'number') {
      amqpSettings.timeout = ocRequest.settings.timeout;
    }
    if (typeof ocRequest.settings.heartbeat === 'number') {
      amqpSettings.heartbeat = ocRequest.settings.heartbeat;
    }
    if (typeof ocRequest.settings.prefetch === 'number') {
      amqpSettings.prefetch = ocRequest.settings.prefetch;
    }
    if (typeof ocRequest.settings.vhost === 'string') {
      amqpSettings.vhost = ocRequest.settings.vhost;
    }
  }

  // bruno item
  const brunoItem: BrunoItem = {
    uid: uuid(),
    type: 'amqp-request',
    seq: info?.seq || 1,
    name: ensureString(info?.name, 'Untitled Request'),
    tags: info?.tags || [],
    request: brunoRequest as any,
    settings: { settings: amqpSettings } as any,
    fileContent: null,
    root: null,
    items: [],
    examples: [],
    filename: null,
    pathname: null
  };

  return brunoItem;
};

export default parseAmqpRequest;
