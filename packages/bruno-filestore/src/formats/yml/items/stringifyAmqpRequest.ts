import type { Item as BrunoItem } from '@usebruno/schema-types/collection/item';
import { stringifyYml } from '../utils';
import { isNonEmptyString } from '../../../utils';

const stringifyAmqpRequest = (item: BrunoItem): string => {
  try {
    const ocRequest: any = {};
    const brunoRequest = item.request as any;

    // info block
    const info: any = {
      name: isNonEmptyString(item.name) ? item.name : 'Untitled Request',
      type: 'amqp'
    };
    if (item.seq) {
      info.seq = item.seq;
    }
    if (item.tags?.length) {
      info.tags = item.tags;
    }
    ocRequest.info = info;

    // amqp block
    const amqp: any = {
      url: isNonEmptyString(brunoRequest?.url) ? brunoRequest.url : ''
    };

    if (isNonEmptyString(brunoRequest?.exchange)) {
      amqp.exchange = brunoRequest.exchange;
    }
    if (isNonEmptyString(brunoRequest?.exchangeType)) {
      amqp.exchangeType = brunoRequest.exchangeType;
    }
    if (isNonEmptyString(brunoRequest?.routingKey)) {
      amqp.routingKey = brunoRequest.routingKey;
    }
    if (isNonEmptyString(brunoRequest?.queue)) {
      amqp.queue = brunoRequest.queue;
    }

    // message body
    if (brunoRequest?.body?.mode === 'amqp' && brunoRequest.body.amqp?.length) {
      const messages = brunoRequest.body.amqp;
      if (messages.length) {
        const msg = messages[0];
        amqp.message = {
          type: msg.type || 'json',
          data: msg.content || ''
        };
      }
    }

    ocRequest.amqp = amqp;

    // settings
    const amqpSettings = (item.settings as any)?.settings;
    if (amqpSettings) {
      ocRequest.settings = {};
      if (amqpSettings.timeout != null) {
        ocRequest.settings.timeout = Number(amqpSettings.timeout) || 0;
      }
      if (amqpSettings.heartbeat != null) {
        ocRequest.settings.heartbeat = Number(amqpSettings.heartbeat) || 0;
      }
      if (amqpSettings.prefetch != null) {
        ocRequest.settings.prefetch = Number(amqpSettings.prefetch) || 0;
      }
      if (isNonEmptyString(amqpSettings.vhost)) {
        ocRequest.settings.vhost = amqpSettings.vhost;
      }
    }

    // docs
    if (isNonEmptyString(brunoRequest?.docs)) {
      ocRequest.docs = brunoRequest.docs;
    }

    return stringifyYml(ocRequest);
  } catch (error) {
    console.error('Error stringifying AMQP request:', error);
    throw error;
  }
};

export default stringifyAmqpRequest;
