const { ipcMain } = require('electron');
const { AmqpClient } = require('@usebruno/requests');
const { cloneDeep, each, get } = require('lodash');
const interpolateVars = require('./interpolate-vars');
const {
  getEnvVars,
  getTreePathFromCollectionToItem,
  mergeHeaders,
  mergeScripts,
  mergeVars,
  mergeAuth,
  getFormattedCollectionOauth2Credentials
} = require('../../utils/collection');
const { getProcessEnvVars } = require('../../store/process-env');
const { interpolateString } = require('./interpolate-string');

const prepareAmqpRequest = async (item, collection, environment, runtimeVariables) => {
  const request = item.draft ? item.draft.request : item.request;
  const collectionRoot = collection?.draft?.root ? get(collection, 'draft.root', {}) : get(collection, 'root', {});
  const brunoConfig = collection.draft?.brunoConfig
    ? get(collection, 'draft.brunoConfig', {})
    : get(collection, 'brunoConfig', {});
  const rawHeaders = cloneDeep(request.headers ?? []);
  const headers = {};

  const scriptFlow = brunoConfig?.scripts?.flow ?? 'sandwich';
  const requestTreePath = getTreePathFromCollectionToItem(collection, item);
  if (requestTreePath && requestTreePath.length > 0) {
    mergeHeaders(collection, request, requestTreePath);
    mergeScripts(collection, request, requestTreePath, scriptFlow);
    mergeVars(collection, request, requestTreePath);
    mergeAuth(collection, request, requestTreePath);
    request.globalEnvironmentVariables = collection?.globalEnvironmentVariables;
    request.oauth2CredentialVariables = getFormattedCollectionOauth2Credentials({
      oauth2Credentials: collection?.oauth2Credentials
    });
  }

  each(rawHeaders, (h) => {
    if (h.enabled && h.name) {
      headers[h.name] = h.value;
    }
  });

  const envVars = getEnvVars(environment);
  const processEnvVars = getProcessEnvVars(collection.uid);
  const collectionVariables = collection?.runtimeVariables || {};

  const amqpRequest = {
    uid: item.uid,
    url: request.url,
    exchange: request.exchange || '',
    exchangeType: request.exchangeType || 'direct',
    routingKey: request.routingKey || '',
    queue: request.queue || '',
    headers,
    body: request.body || { mode: 'amqp', amqp: [] },
    auth: request.auth || {},
    envVars,
    processEnvVars,
    collectionVariables
  };

  // Interpolate variables in AMQP-specific fields
  const interpolationOptions = {
    envVars,
    collectionVariables,
    processEnvVars,
    runtimeVariables: runtimeVariables || {}
  };
  amqpRequest.url = interpolateString(amqpRequest.url, interpolationOptions);
  amqpRequest.exchange = interpolateString(amqpRequest.exchange, interpolationOptions);
  amqpRequest.routingKey = interpolateString(amqpRequest.routingKey, interpolationOptions);
  amqpRequest.queue = interpolateString(amqpRequest.queue, interpolationOptions);

  return amqpRequest;
};

let amqpClient;

const registerAmqpEventHandlers = (window) => {
  const sendEvent = (eventName, ...args) => {
    if (window && !window.isDestroyed() && window.webContents && !window.webContents.isDestroyed()) {
      window.webContents.send(eventName, ...args);
    } else {
      console.warn(`Unable to send message "${eventName}": Window not available`);
    }
  };

  amqpClient = new AmqpClient(sendEvent);

  // Connect to AMQP broker
  ipcMain.handle(
    'renderer:amqp:connect',
    async (event, { request, collection, environment, runtimeVariables, settings }) => {
      try {
        const requestCopy = cloneDeep(request);
        const preparedRequest = await prepareAmqpRequest(
          { uid: request.uid, request: requestCopy, draft: null },
          collection,
          environment,
          runtimeVariables
        );

        const connectOptions = {
          heartbeat: settings?.heartbeat || 0,
          prefetch: settings?.prefetch || 0,
          vhost: settings?.vhost || '/'
        };

        await amqpClient.connect(
          preparedRequest.uid,
          collection.uid,
          preparedRequest.url,
          connectOptions
        );

        // Declare exchange if specified
        if (preparedRequest.exchange) {
          await amqpClient.declareExchange(
            preparedRequest.uid,
            collection.uid,
            preparedRequest.exchange,
            preparedRequest.exchangeType || 'direct'
          );
        }

        // Declare queue if specified
        let queueName = preparedRequest.queue;
        if (queueName) {
          const queueResult = await amqpClient.declareQueue(
            preparedRequest.uid,
            collection.uid,
            queueName
          );
          queueName = queueResult.queue;
        }

        // Bind queue to exchange if both are specified
        if (queueName && preparedRequest.exchange) {
          await amqpClient.bindQueue(
            preparedRequest.uid,
            collection.uid,
            queueName,
            preparedRequest.exchange,
            preparedRequest.routingKey || ''
          );
        }

        return { success: true };
      } catch (error) {
        console.error('Error connecting to AMQP broker:', error);
        sendEvent('main:amqp:error', request.uid, collection.uid, {
          message: error.message,
          timestamp: Date.now()
        });
        return { success: false, error: error.message };
      }
    }
  );

  // Publish a message
  ipcMain.handle(
    'renderer:amqp:publish',
    async (event, { requestUid, collectionUid, exchange, routingKey, queue, content, options }) => {
      try {
        if (exchange) {
          await amqpClient.publish(requestUid, collectionUid, exchange, routingKey || '', content, options || {});
        } else if (queue) {
          await amqpClient.sendToQueue(requestUid, collectionUid, queue, content, options || {});
        } else {
          throw new Error('Either exchange or queue must be specified for publishing');
        }
        return { success: true };
      } catch (error) {
        console.error('Error publishing AMQP message:', error);
        return { success: false, error: error.message };
      }
    }
  );

  // Start consuming from a queue
  ipcMain.handle(
    'renderer:amqp:start-consuming',
    async (event, { requestUid, collectionUid, queue, options }) => {
      try {
        const result = await amqpClient.consume(requestUid, collectionUid, queue, options || {});
        return { success: true, consumerTag: result.consumerTag };
      } catch (error) {
        console.error('Error starting AMQP consumer:', error);
        return { success: false, error: error.message };
      }
    }
  );

  // Stop consuming
  ipcMain.handle(
    'renderer:amqp:stop-consuming',
    async (event, { requestUid, collectionUid }) => {
      try {
        await amqpClient.stopConsuming(requestUid, collectionUid);
        return { success: true };
      } catch (error) {
        console.error('Error stopping AMQP consumer:', error);
        return { success: false, error: error.message };
      }
    }
  );

  // Disconnect
  ipcMain.handle(
    'renderer:amqp:disconnect',
    async (event, { requestUid, collectionUid }) => {
      try {
        await amqpClient.disconnect(requestUid, collectionUid);
        return { success: true };
      } catch (error) {
        console.error('Error disconnecting AMQP:', error);
        return { success: false, error: error.message };
      }
    }
  );

  // Get connection status
  ipcMain.handle(
    'renderer:amqp:connection-status',
    (event, { requestUid, collectionUid }) => {
      try {
        const status = amqpClient.getStatus(requestUid, collectionUid);
        return { success: true, status };
      } catch (error) {
        console.error('Error getting AMQP connection status:', error);
        return { success: false, error: error.message, status: { connected: false, hasChannel: false, consuming: false } };
      }
    }
  );
};

module.exports = {
  registerAmqpEventHandlers,
  amqpClient,
  prepareAmqpRequest
};
