import React, { useState, useEffect, useCallback } from 'react';
import toast from 'react-hot-toast';

const AmqpResponsePane = ({ item, collection }) => {
  const [messages, setMessages] = useState([]);
  const [isConsuming, setIsConsuming] = useState(false);

  const queue = item.draft?.request?.queue || item.request?.queue || '';

  const handlePublish = useCallback(async () => {
    try {
      const request = item.draft?.request || item.request;
      const body = request?.body?.amqp?.[0]?.content || '{}';
      const exchange = request?.exchange || '';
      const routingKey = request?.routingKey || '';
      const queueName = request?.queue || '';

      const { ipcRenderer } = window;
      const result = await ipcRenderer.invoke('renderer:amqp:publish', {
        requestUid: item.uid,
        collectionUid: collection.uid,
        exchange,
        routingKey,
        queue: queueName,
        content: body,
        options: {}
      });

      if (result.success) {
        setMessages((prev) => [...prev, {
          direction: 'out',
          content: body,
          exchange: exchange || '(default)',
          routingKey: routingKey || queueName,
          timestamp: Date.now()
        }]);
      } else {
        toast.error(`Publish failed: ${result.error}`);
      }
    } catch (err) {
      toast.error(`Publish error: ${err.message}`);
    }
  }, [item, collection]);

  const handleStartConsuming = useCallback(async () => {
    try {
      const { ipcRenderer } = window;
      const result = await ipcRenderer.invoke('renderer:amqp:start-consuming', {
        requestUid: item.uid,
        collectionUid: collection.uid,
        queue,
        options: {}
      });
      if (result.success) {
        setIsConsuming(true);
        toast.success('Started consuming');
      } else {
        toast.error(`Failed to consume: ${result.error}`);
      }
    } catch (err) {
      toast.error(`Consume error: ${err.message}`);
    }
  }, [item.uid, collection.uid, queue]);

  const handleStopConsuming = useCallback(async () => {
    try {
      const { ipcRenderer } = window;
      await ipcRenderer.invoke('renderer:amqp:stop-consuming', {
        requestUid: item.uid,
        collectionUid: collection.uid
      });
      setIsConsuming(false);
      toast.success('Stopped consuming');
    } catch (err) {
      toast.error(`Stop error: ${err.message}`);
    }
  }, [item.uid, collection.uid]);

  // Listen for incoming messages
  useEffect(() => {
    const { ipcRenderer } = window;
    const handleMessageReceived = (event, requestUid, collectionUid, data) => {
      if (requestUid === item.uid && collectionUid === collection.uid) {
        setMessages((prev) => [...prev, {
          direction: 'in',
          content: data.content,
          routingKey: data.fields?.routingKey || '',
          exchange: data.fields?.exchange || '',
          timestamp: data.timestamp || Date.now()
        }]);
      }
    };

    const handlePublished = (event, requestUid, collectionUid, data) => {
      if (requestUid === item.uid && collectionUid === collection.uid) {
        // Already handled in publish handler above
      }
    };

    const handleConsumerStopped = (event, requestUid, collectionUid) => {
      if (requestUid === item.uid && collectionUid === collection.uid) {
        setIsConsuming(false);
      }
    };

    ipcRenderer.on('main:amqp:message-received', handleMessageReceived);
    ipcRenderer.on('main:amqp:message-published', handlePublished);
    ipcRenderer.on('main:amqp:consumer-stopped', handleConsumerStopped);

    return () => {
      ipcRenderer.removeListener('main:amqp:message-received', handleMessageReceived);
      ipcRenderer.removeListener('main:amqp:message-published', handlePublished);
      ipcRenderer.removeListener('main:amqp:consumer-stopped', handleConsumerStopped);
    };
  }, [item.uid, collection.uid]);

  return (
    <div className="flex flex-col h-full px-4 py-2">
      <div className="flex items-center gap-2 mb-3">
        <button
          className="px-3 py-1 text-xs font-medium rounded bg-blue-600 text-white hover:bg-blue-700"
          onClick={handlePublish}
        >
          Publish
        </button>
        {!isConsuming ? (
          <button
            className="px-3 py-1 text-xs font-medium rounded bg-green-600 text-white hover:bg-green-700"
            onClick={handleStartConsuming}
            disabled={!queue}
            title={!queue ? 'Set a queue name first' : 'Start consuming messages'}
          >
            Start Consuming
          </button>
        ) : (
          <button
            className="px-3 py-1 text-xs font-medium rounded bg-red-600 text-white hover:bg-red-700"
            onClick={handleStopConsuming}
          >
            Stop Consuming
          </button>
        )}
        <button
          className="px-3 py-1 text-xs font-medium rounded border hover:bg-gray-100 dark:hover:bg-gray-700"
          onClick={() => setMessages([])}
        >
          Clear
        </button>
        <span className="text-xs opacity-50 ml-auto">{messages.length} message(s)</span>
      </div>

      <div className="flex-1 overflow-y-auto border rounded p-2 text-xs font-mono">
        {messages.length === 0 && (
          <div className="text-center opacity-40 mt-8">
            No messages yet. Connect and publish or start consuming.
          </div>
        )}
        {messages.map((msg, idx) => (
          <div key={idx} className={`mb-2 p-2 rounded ${msg.direction === 'in' ? 'bg-green-50 dark:bg-green-900/20' : 'bg-blue-50 dark:bg-blue-900/20'}`}>
            <div className="flex items-center gap-2 mb-1">
              <span className={`text-[10px] font-bold ${msg.direction === 'in' ? 'text-green-600' : 'text-blue-600'}`}>
                {msg.direction === 'in' ? '← RECV' : '→ SENT'}
              </span>
              {msg.routingKey && (
                <span className="text-[10px] opacity-60">key={msg.routingKey}</span>
              )}
              <span className="text-[10px] opacity-40 ml-auto">
                {new Date(msg.timestamp).toLocaleTimeString()}
              </span>
            </div>
            <pre className="whitespace-pre-wrap break-all text-[11px]">{msg.content}</pre>
          </div>
        ))}
      </div>
    </div>
  );
};

export default AmqpResponsePane;
